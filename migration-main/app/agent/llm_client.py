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

def generate_sqls(mapping_rule, last_error=None, last_sql=None):
    """
    OpenAI 호환 API를 호출하여 Oracle 11g 마이그레이션 SQL과 검증 SQL을 생성합니다.
    """
    client = get_client()
    model_name = os.getenv("LLM_MODEL") or "gpt-4o-mini"
    
    from_table = mapping_rule.fr_table
    to_table = mapping_rule.to_table
    
    # 컬럼 정보 정리
    details = mapping_rule.details
    mapping_info = "\n".join([f"- {d.fr_col} -> {d.to_col}" for d in details])
    
    # 프롬프트 구성 (Oracle 11g 전문가 페르소나 적용)
    prompt = f"""
    당신은 Oracle 21c 데이터 마이그레이션 전문가입니다. 
    제시된 매핑 규칙을 기반으로 마이그레이션 SQL과 정합성 검증 SQL을 JSON 형식으로 생성하십시오.

    [매핑 규칙]
    - 소스 테이블: {from_table}
    - 타겟 테이블: {to_table}
    - 컬럼 매핑 정보:
    {mapping_info}

    [필수 요구사항]
    1. Migration SQL: 
       - 전달받은 소스 테이블명('{from_table}')과 타겟 테이블명('{to_table}')을 절대적으로 준수하십시오.
       - 소스 테이블명에 'HR.' 등의 접두어가 없다면 임의로 붙이지 마십시오. (hallucination 주의)
       - 타겟 테이블이 없으면 생성하는 DDL과 데이터를 옮기는 SELECT INSERT DML을 포함하십시오.
       - Oracle 21c 환경이므로 레거시 호환 문법을 사용하십시오.
       - 여러 SQL 문장이 포함될 경우 각 문장은 반드시 슬래시(/) 단독 라인으로 구분하십시오.
       
    2. Verification SQL (2단계 정합성 검증):
       - 반드시 위에서 사용한 정확한 소스/타겟 테이블명만 사용하십시오.
       - 1단계: 전체 결과 건수(Row Count) 비교.
       - 2단계: 매핑된 모든 컬럼에 대해 Null이 아닌(유효한) 데이터의 개수가 일치하는지 비교.
       - 모든 차이값의 절대값 합계를 구하여 단일 컬럼 'DIFF'로 반환하는 SQL을 작성하십시오.
       - 예시: SELECT ABS((소스전체)-(타겟전체)) + ABS((소스컬럼1건수)-(타겟컬럼1건수)) + ... AS DIFF FROM DUAL

    응답은 반드시 'migration_sql'과 'verification_sql' 키를 가진 JSON 객체여야 합니다.
    """
    
    if last_error:
        prompt += f"\n\n[이전 실행 실패 피드백]\n- 실패한 SQL: {last_sql}\n- 발생한 에러: {last_error}\n- 작업: 위 에러를 분석하여 올바르게 수정한 쿼리를 다시 생성하십시오."

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that generates Oracle SQL in JSON format."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"} # JSON 모드 강제
        )
        
        result_text = response.choices[0].message.content
        result = json.loads(result_text)
        
        migration_sql = result.get("migration_sql", "")
        verification_sql = result.get("verification_sql", "")
        
        logger.info(f"[LLM] SQL 생성 완료 (Model: {model_name})")
        return migration_sql, verification_sql

    except Exception as e:
        logger.error(f"[LLM] API 호출 중 에러: {e}")
        # 예외 타입에 따른 세분화 처리는 필요 시 추가
        raise LLMConnectionError(f"LLM 연결 실패: {str(e)}")
