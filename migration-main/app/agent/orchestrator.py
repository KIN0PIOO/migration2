import time
import os
import re
from app.core.logger import logger
from app.core.exceptions import (
    LLMBaseError, LLMAuthenticationError, LLMTokenLimitError, LLMInvalidRequestError,
    DBSqlError, VerificationFailError, BatchAbortError
)
from app.agent.llm_client import generate_sqls
from app.agent.executor import execute_migration, drop_table_if_exists
from app.agent.verifier import execute_verification
from app.domain.mapping.repository import update_job_status, increment_batch_count
from app.domain.history.repository import log_generated_sql, log_business_history
from app.core.db import fetch_table_ddl

def _extract_table_names(fr_table: str) -> list:
    """FR_TABLE 표현식(JOIN 포함)에서 실제 테이블명만 추출합니다."""
    parts = re.split(
        r'\b(?:(?:LEFT|RIGHT|FULL|INNER|CROSS)\s+(?:OUTER\s+)?)?JOIN\b',
        fr_table, flags=re.IGNORECASE
    )
    tables = []
    for part in parts:
        # ON 조건 이후 제거
        part = re.split(r'\bON\b', part, flags=re.IGNORECASE)[0].strip()
        tokens = part.split()
        if tokens and tokens[0].upper() not in ('SELECT', 'WITH', 'FROM', '('):
            tables.append(tokens[0])
    return tables


class MigrationOrchestrator:
    def __init__(self):
        self.mig_kind = os.getenv("MIG_KIND", "DB_MIG")

    def process_job(self, NEXT_SQL_INFO):
        logger.info(f"\n==========================================")
        logger.info(f"[JOB_START] 대상 작업(map_id={NEXT_SQL_INFO.map_id}) 프로세스 시작")
        
        # 0. BATCH_COUNT 증가 (작업 시작 기록)
        increment_batch_count(NEXT_SQL_INFO.map_id)
        
        job_start_time = time.time()
        llm_retry_count = 0
        db_attempts = 1
        max_attempts = 3
        last_error = None
        last_sql = None

        # 소스 테이블 DDL 정보 조회 (읽기 전용, 루프 밖에서 1회만 실행)
        # FR_TABLE이 JOIN 표현식일 수 있으므로 테이블명만 파싱하여 각각 조회
        source_ddl = {}
        for tbl_name in _extract_table_names(NEXT_SQL_INFO.fr_table):
            rows = fetch_table_ddl(tbl_name)
            if rows:
                source_ddl[tbl_name] = rows
                logger.info(f"[DDL_FETCH] map_id={NEXT_SQL_INFO.map_id} | {tbl_name} 컬럼 {len(rows)}개 조회 완료")
            else:
                logger.warning(f"[DDL_FETCH] map_id={NEXT_SQL_INFO.map_id} | {tbl_name} DDL 조회 실패 — 타입 정보 없이 진행")
        if not source_ddl:
            source_ddl = None

        while True:
            try:
                # 1. SQL 생성 (DDL, Migration, Verification)
                NEXT_SQL_INFO.retry_count = db_attempts - 1
                logger.debug(f"[STEP_START] map_id={NEXT_SQL_INFO.map_id} | [Total Attempt {db_attempts}/{max_attempts}] | 1. LLM 쿼리 생성 요청")
                ddl_sql, migration_sql, v_sql = generate_sqls(NEXT_SQL_INFO, last_error, last_sql, source_ddl)
                
                # DB 기록 (MIG_SQL에는 INSERT만 저장하고, DDL은 메모리에서만 사용)
                log_generated_sql(NEXT_SQL_INFO.map_id, migration_sql, v_sql)
                last_sql = migration_sql
                
                # 2. 클린업 및 실행
                # 2a. 기존 테이블 삭제 (Clean Retry 환경 조성)
                to_table = NEXT_SQL_INFO.to_table
                logger.debug(f"[STEP_START] map_id={NEXT_SQL_INFO.map_id} | 2a. 기존 타겟 테이블 삭제")
                drop_table_if_exists(to_table)
                
                # 2b. DDL 실행 (테이블 생성)
                if ddl_sql:
                    logger.debug(f"[STEP_START] map_id={NEXT_SQL_INFO.map_id} | 2b. 타겟 테이블 생성 (DDL)")
                    execute_migration(ddl_sql)
                
                # 2c. Migration 실행 (데이터 이관)
                logger.debug(f"[STEP_START] map_id={NEXT_SQL_INFO.map_id} | 2c. 데이터 이관 실행 (DML)")
                execute_migration(migration_sql)
                
                # 3. 검증 실행
                if v_sql:
                    logger.debug(f"[STEP_START] map_id={NEXT_SQL_INFO.map_id} | 3. 데이터 정합성 검증")
                    is_valid, v_msg = execute_verification(v_sql)
                    if not is_valid:
                        raise VerificationFailError(f"데이터 불일치: {v_msg}")
                
                # 성공 처리
                elapsed = int(time.time() - job_start_time)
                update_job_status(NEXT_SQL_INFO.map_id, "PASS", elapsed, db_attempts)
                log_business_history(NEXT_SQL_INFO.map_id, "INFO", "INFO", "VERIFY", "PASS", "Migration Success", db_attempts, self.mig_kind)
                logger.info(f"[JOB_PASS] map_id={NEXT_SQL_INFO.map_id} | >>> 마이그레이션 통과 <<<")
                return

            except (LLMAuthenticationError, LLMTokenLimitError, LLMInvalidRequestError) as e:
                logger.error(f"[BATCH_ABORT] map_id={NEXT_SQL_INFO.map_id} | >>> {e.__class__.__name__} 발생. 배치 즉시 중단 <<<")
                raise BatchAbortError(f"LLM 치명적 에러: {str(e)}") from e

            except LLMBaseError as e:
                llm_retry_count += 1
                if llm_retry_count > 2:
                    raise BatchAbortError(f"LLM 재시도 초과: {str(e)}") from e
                logger.warning(f"[LLM_RETRY] map_id={NEXT_SQL_INFO.map_id} | retry={llm_retry_count} | {str(e)}")
                time.sleep(1)

            except (DBSqlError, VerificationFailError) as e:
                # ORA-00942 (테이블 미존재) 에러 등이 여기서 잡힐 수 있음
                step_name = "SQL_EXEC" if isinstance(e, DBSqlError) else "VERIFY"
                logger.error(f"[BUSINESS_RETRY] map_id={NEXT_SQL_INFO.map_id} | attempt={db_attempts} | {str(e)}")
                log_business_history(NEXT_SQL_INFO.map_id, "ROW_ERROR", "WARN", step_name, "FAIL", str(e), db_attempts, self.mig_kind)
                
                last_error = str(e)
                if db_attempts < max_attempts:
                    db_attempts += 1
                    time.sleep(1)
                else:
                    elapsed = int(time.time() - job_start_time)
                    logger.error(f"[JOB_FAIL] map_id={NEXT_SQL_INFO.map_id} | >>> 최대 시도({max_attempts}회) 도달. FAIL 판정 <<<")
                    update_job_status(NEXT_SQL_INFO.map_id, "FAIL", elapsed, db_attempts)
                    log_business_history(NEXT_SQL_INFO.map_id, "JOB_FAIL", "ERROR", step_name, "FAIL", "Max Attempts Reached", db_attempts, self.mig_kind)
                    return
