import time
from app.core.logger import logger
from app.core.exceptions import (
    LLMBaseError, LLMAuthenticationError, LLMTokenLimitError, LLMInvalidRequestError,
    DBSqlError, VerificationFailError, BatchAbortError
)
from app.agent.llm_client import generate_sqls
from app.agent.executor import execute_migration
from app.agent.verifier import execute_verification
from app.agent.verifier import execute_verification
from app.domain.mapping.repository import update_job_status, increment_batch_count
from app.domain.history.repository import log_generated_sql, log_business_history

class MigrationOrchestrator:
    def process_job(self, mapping_rule):
        logger.info(f"\n==========================================")
        logger.info(f"[JOB_START] 대상 작업(map_id={mapping_rule.map_id}) 프로세스 시작")
        
        # 0. BATCH_COUNT 증가 (작업 시작 기록)
        increment_batch_count(mapping_rule.map_id)
        
        job_start_time = time.time()
        llm_retry_count = 0
        db_attempts = 1
        max_attempts = 3
        last_error = None
        last_sql = None
        
        while True:
            try:
                # 1. SQL 생성 (피드백 반영)
                # llm_client가 현재 몇 번째 시도인지 알 수 있도록 retry_count 동기화 (0부터 시작)
                mapping_rule.retry_count = db_attempts - 1
                logger.debug(f"[STEP_START] map_id={mapping_rule.map_id} | [Total Attempt {db_attempts}/{max_attempts}] | 1. LLM 쿼리 생성 요청")
                migration_sql, verification_sql = generate_sqls(mapping_rule, last_error, last_sql)
                last_sql = migration_sql # 이번 시도에 사용된 SQL 저장
                log_generated_sql(mapping_rule.map_id, migration_sql, verification_sql)
                
                # 2. 마이그레이션 실행
                logger.debug(f"[STEP_START] map_id={mapping_rule.map_id} | 2. 쿼리 파싱 및 DB 실행")
                execute_migration(migration_sql)
                
                # 3. 검증 실행
                if verification_sql:
                    logger.debug(f"[STEP_START] map_id={mapping_rule.map_id} | 3. 데이터 정합성 검증")
                    is_valid, v_msg = execute_verification(verification_sql)
                    if not is_valid:
                        raise VerificationFailError(f"데이터 불일치: {v_msg}")
                
                # 실행 무사고 시 바로 PASS 처리
                elapsed = int(time.time() - job_start_time)
                update_job_status(mapping_rule.map_id, "PASS", elapsed, db_attempts)
                log_business_history(mapping_rule.map_id, "INFO", "INFO", "VERIFY", "PASS", "Migration Success", db_attempts)
                logger.info(f"[JOB_PASS] map_id={mapping_rule.map_id} | >>> 마이그레이션 통과 <<<")
                return

            except (LLMAuthenticationError, LLMTokenLimitError, LLMInvalidRequestError) as e:
                logger.error(f"[BATCH_ABORT] map_id={mapping_rule.map_id} | >>> {e.__class__.__name__} 발생. 배치 즉시 중단 (작업은 Y로 유지) <<<")
                raise BatchAbortError(f"LLM 致命 에러: {str(e)}") from e

            except LLMBaseError as e:
                llm_retry_count += 1
                if llm_retry_count > 2: # Max 3 LLM attempts
                    raise BatchAbortError(f"LLM 재시도 초과: {str(e)}") from e
                logger.warning(f"[LLM_RETRY] map_id={mapping_rule.map_id} | retry={llm_retry_count} | {str(e)}")
                time.sleep(1)

            except (DBSqlError, VerificationFailError) as e:
                step_name = "SQL_EXEC" if isinstance(e, DBSqlError) else "VERIFY"
                logger.error(f"[BUSINESS_RETRY] map_id={mapping_rule.map_id} | attempt={db_attempts} | {str(e)}")
                log_business_history(mapping_rule.map_id, "ROW_ERROR", "WARN", step_name, "FAIL", str(e), db_attempts)
                
                last_error = str(e)
                if db_attempts < max_attempts:
                    db_attempts += 1
                    time.sleep(1)
                else:
                    elapsed = int(time.time() - job_start_time)
                    logger.error(f"[JOB_FAIL] map_id={mapping_rule.map_id} | >>> 최대 시도({max_attempts}회) 도달. FAIL 판정 <<<")
                    update_job_status(mapping_rule.map_id, "FAIL", elapsed, db_attempts)
                    log_business_history(mapping_rule.map_id, "JOB_FAIL", "ERROR", step_name, "FAIL", "Max Attempts Reached", db_attempts)
                    return
