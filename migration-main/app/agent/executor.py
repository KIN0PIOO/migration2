from app.core.exceptions import DBSqlError
from app.core.db import get_connection
from app.core.logger import logger
import oracledb
import re

def execute_migration(sql_script: str):
    """생성된 SQL 스크립트를 Oracle DB 엔진에 실행 (PL/SQL 블록 및 일반 SQL 구분 강화)"""
    logger.debug(f"[Executor] 실제 쿼리 실행 시작: {sql_script[:50]}...")
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. '/' (슬래시) 단독 라인 기준으로 크게 분할
            statements = re.split(r'^\s*/\s*$', sql_script, flags=re.MULTILINE)
            
            for stmt in statements:
                clean_stmt = stmt.strip()
                if not clean_stmt:
                    continue

                # 주석 제거하고 실질적인 시작 단어 확인
                content_only = re.sub(r'--.*$', '', clean_stmt, flags=re.MULTILINE)
                content_only = re.sub(r'/\*.*?\*/', '', content_only, flags=re.DOTALL).strip()
                
                # 시작 단어가 BEGIN 이나 DECLARE 인지 확인
                if re.match(r'^(BEGIN|DECLARE)', content_only, re.IGNORECASE):
                    # PL/SQL 블록은 전체를 하나로 실행하되, 끝에 개행을 추가하여 EOF 에러 방지
                    logger.debug(f"[Executor] PL/SQL Block execution (len={len(clean_stmt)})")
                    try:
                        # Oracle 11g 등 일부 환경에서 PL/SQL 블록은 끝에 개행이 있어야 안전함
                        cursor.execute(clean_stmt + "\n")
                    except oracledb.DatabaseError as e:
                        raise e
                else:
                    # 일반 SQL은 세미콜론으로 쪼개서 실행
                    commands = [c.strip() for c in clean_stmt.split(';') if c.strip()]
                    for sub_cmd in commands:
                        if sub_cmd.startswith("--"):
                            continue
                        logger.info(f"[Executor] Executing SQL: {sub_cmd[:70]}...")
                        try:
                            # 일반 SQL은 마지막에 세미콜론이 없어야 함 (cursor.execute 규칙)
                            cursor.execute(sub_cmd)
                        except oracledb.DatabaseError as e:
                            # ORA-00955: 기존의 객체가 이름을 사용하고 있습니다.
                            # CREATE 문 실행 시 이미 테이블/인덱스가 있다면 무시하고 진행
                            if "ORA-00955" in str(e) and sub_cmd.strip().upper().startswith("CREATE"):
                                logger.warning(f"[Executor] 객체가 이미 존재하여 건너뜁니다: {sub_cmd[:50]}...")
                                continue
                            logger.error(f"[Executor] Command failed: {sub_cmd[:50]}...")
                            raise e
            
            conn.commit()
            logger.info(f"[Executor] All commands executed and committed successfully.")
            
    except Exception as e:
        logger.error(f"[Executor] 마이그레이션 쿼리 실패: {str(e)}")
        raise DBSqlError(f"Oracle 쿼리 실행 에러: {str(e)}")
