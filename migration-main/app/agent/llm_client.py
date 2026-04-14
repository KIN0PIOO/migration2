import os
import json
from openai import OpenAI
from app.core.exceptions import LLMConnectionError, LLMAuthenticationError, LLMTokenLimitError
from app.core.logger import logger
from dotenv import load_dotenv

# .env 로드 (루트 경로 기준)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env")
load_dotenv(env_path)

def get_client():
    """OpenAI 호환 모델 클라이언트를 반환합니다."""
    api_key = os.getenv("OPEN_API_KEY")
    base_url = os.getenv("LLM_BASE_URL")
    
    if not api_key:
        error_msg = f"API Key(OPEN_API_KEY)가 설정되지 않았습니다. (Path: {env_path})"
        logger.error(f"[LLM] {error_msg}")
        raise LLMAuthenticationError(error_msg)
    
    # OpenAI 호환 클라이언트 초기화 (base_url이 있으면 사내 Gateway 등으로 연결)
    return OpenAI(
        api_key=api_key,
        base_url=base_url if base_url else None
    )

def generate_sqls(NEXT_SQL_INFO, last_error=None, last_sql=None):
    """
    OpenAI 호환 API를 호출하여 Oracle 21c 마이그레이션 SQL들을 생성합니다.
    (DDL, Migration, Verification 분리)
    """
    client = get_client()
    model_name = os.getenv("LLM_MODEL") or "gpt-4o-mini"
    
    from_table = NEXT_SQL_INFO.fr_table
    to_table = NEXT_SQL_INFO.to_table
    
    # 컬럼 정보 정리
    details = NEXT_SQL_INFO.details
    mapping_info = "\n".join([f"- {d.fr_col} -> {d.to_col}" for d in details])
    
    # 프롬프트 구성 (Oracle 21c 전문가 페르소나 적용)
    prompt = f"""
    당신은 Oracle 데이터 마이그레이션 전문가입니다. 
    제시된 매핑 규칙을 기반으로 (1) 테이블 생성 DDL, (2) 데이터 이관 DML, (3) 정합성 검증 SQL을 JSON 형식으로 각각 생성하십시오.

    [매핑 규칙]
    - 소스 테이블: {from_table}
    - 타겟 테이블: {to_table}
    - 컬럼 매핑 정보:
    {mapping_info}

    [필수 요구사항]
    1. ddl_sql (Schema Phase):
       - 타겟 테이블('{to_table}')을 생성하는 'CREATE TABLE' 문장만 포함하십시오.
       - 소스 컬럼의 타입을 Oracle 21c 환경에 적합하게 변환하십시오.
       - DROP 문장이나 TRUNCATE 문장은 포함하지 마십시오. (에이전트 로직에서 처리함)

    2. migration_sql (Migration Phase):
       - 데이터를 실제로 옮기는 'INSERT INTO ... SELECT ...' 문장만 포함하십시오.
       - 반드시 위에서 정의한 타겟 테이블('{to_table}')과 소스 테이블('{from_table}')을 사용하십시오.
       - 소스 테이블명에 'HR.' 등의 접두어가 없다면 임의로 붙이지 마십시오. (hallucination 주의)
       - 데이터 타입 변환(TO_DATE 등)이 필요한 경우 인라인으로 처리하십시오.
       - 이 단계는 테이블이 이미 생성된 상태에서 실행됩니다.

    3. verification_sql (Verification Phase):
       - 소스/타겟 테이블의 데이터 정합성을 비교하는 단일 SELECT 문장을 작성하십시오.
       - 1단계: 전체 결과 건수(Row Count) 비교.
       - 2단계: 매핑된 컬럼들의 Null이 아닌 데이터 개수 합산 대조.
       - 모든 차이값의 절대값 합계를 구하여 단일 컬럼 'DIFF'로 반환하십시오.
       - 'DIFF'가 0이면 완벽한 일치로 간주됩니다.
       - 예시: SELECT ABS((소스건수)-(타겟건수)) + ABS((소스컬럼1건수)-(타겟컬럼1건수)) AS DIFF FROM DUAL

    4. 공통:
       - 날짜 데이터 처리 시 반드시 TO_DATE 함수와 'YYYY-MM-DD HH24:MI:SS' 포맷을 사용하십시오.
       - 여러 SQL 문장이 포함될 경우 각 문장은 반드시 슬래시(/) 단독 라인으로 구분하십시오.

    응답은 반드시 'ddl_sql', 'migration_sql', 'verification_sql' 키를 가진 JSON 객체여야 합니다.
    """
    
    # [추가] 인간 전문가가 직접 수정한 정답 SQL이 있다면 프롬프트에 반영
    if NEXT_SQL_INFO.correct_sql:
        logger.info(f"[LLM] map_id={NEXT_SQL_INFO.map_id} | 인간 전문가의 정답 SQL을 프롬프트에 반영합니다.")
        prompt += f"\n\n[인간 전문가가 검증한 정답 SQL 예시]\n{NEXT_SQL_INFO.correct_sql}\n"
        prompt += "- 위 예시의 패턴을 참고하여 ddl_sql, migration_sql, verification_sql로 나누어 생성하십시오.\n"

    if last_error:
        prompt += f"\n\n[이전 실행 실패 피드백]\n- 실패한 SQL: {last_sql}\n- 발생한 에러: {last_error}\n- 작업: 위 에러를 분석하여 올바르게 수정한 쿼리들을 다시 생성하십시오."

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that generates Oracle SQL in JSON format."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        
        ddl_sql = result.get("ddl_sql", "")
        migration_sql = result.get("migration_sql", "")
        verification_sql = result.get("verification_sql", "")
        
        # (Post-processing) 리스트 형태일 경우 문자열로 병합
        def merge_list(val):
            if isinstance(val, list):
                return "\n/\n".join(val)
            return val

        logger.info(f"[LLM] SQL 생성 완료 (Model: {model_name})")
        return merge_list(ddl_sql), merge_list(migration_sql), merge_list(verification_sql)

    except Exception as e:
        logger.error(f"[LLM] API 호출 중 에러: {e}")
        # 예외 타입에 따른 세분화 처리는 필요 시 추가
        raise LLMConnectionError(f"LLM 연결 실패: {str(e)}")
