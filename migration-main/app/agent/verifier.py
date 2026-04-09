from app.core.exceptions import DBSqlError
from app.core.db import get_connection
from app.core.logger import logger

def execute_verification(sql: str) -> tuple[bool, str]:
    """양 DB의 정합성을 대조하는 검증 SQL 실행"""
    logger.debug(f"[Verifier] 실제 검증 쿼리 실행 시작: {sql[:50]}...")
    
    try:
        clean_sql = sql.strip().rstrip(";")
        
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(clean_sql)
            rows = cursor.fetchall()
            
            if not rows:
                return True, "No mismatch found (Empty ResultSet)"
            
            for row in rows:
                for col_val in row:
                    if col_val and str(col_val) != "0":
                        return False, f"Mismatch found: {row}"
                        
            return True, "All Verification Passed"
            
    except Exception as e:
        logger.error(f"[Verifier] 검증 쿼리 실행 에러: {str(e)}")
        return False, f"Verification Query Error: {str(e)}"
