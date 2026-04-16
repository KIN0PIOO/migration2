# AI 기반 Oracle 데이터 마이그레이션 에이전트

Oracle DB의 매핑 룰 테이블(`NEXT_MIG_INFO`)을 폴링하여, LLM이 DDL/DML/검증 SQL을 자동 생성하고 실행·검증·재시도하는 자율 마이그레이션 에이전트입니다.

---

## 주요 기능

- **자동 SQL 생성**: LLM(OpenAI 호환 API)이 컬럼 매핑 정보와 소스 테이블 DDL을 바탕으로 타겟 테이블 생성(DDL), 데이터 이관(DML), 정합성 검증(SQL) 3종을 자동 생성
- **자동 실행 및 검증**: 생성된 SQL을 Oracle DB에 실행하고 DIFF=0 여부로 데이터 정합성 검증
- **자동 재시도**: 실패 시 에러 메시지를 LLM에 피드백하여 최대 3회까지 SQL을 자동 교정·재시도
- **스케줄 기반 폴링**: APScheduler로 10초 주기로 DB를 스캔하여 PENDING 작업을 자동 처리
- **실행 이력 기록**: 모든 시도(성공/실패/에러)를 `NEXT_MIG_LOG`에 기록

---

## 시스템 아키텍처

```
[APScheduler 10초 폴링]
        |
   scheduler.py          ← DB에서 PENDING 작업 조회
        |
  orchestrator.py        ← 작업 파이프라인 총괄
   ├─ llm_client.py      ← LLM API 호출 → DDL / MIG_SQL / VERIFY_SQL 생성
   ├─ executor.py        ← Oracle DB 실행 (DDL → INSERT)
   └─ verifier.py        ← 정합성 검증 (DIFF=0 확인)
        |
  [NEXT_MIG_INFO 상태 업데이트 + NEXT_MIG_LOG 기록]
```

---

## 디렉토리 구조

```
migration-main/
├── .env                          # DB 접속 정보 및 LLM 설정
├── META_MIGRATION_TO_21C.sql     # 메타데이터 테이블 초기화 스크립트
├── HR_MIGRATION_TO_21C.sql       # HR 타겟 테이블 생성 스크립트
├── HR_EXPORT_DATA.sql            # HR 소스 데이터 익스포트
├── FIX_HR_PREFIX_FOR_SCOTT.sql   # scott 계정 HR 테이블 접두어 수정
├── ADD_SIMPLE_TEST_CASES.sql     # 검증용 Simple 케이스 5건 추가
├── generate_hr_migration.py      # HR 마이그레이션 SQL 생성 유틸
├── generate_meta_migration.py    # 메타 마이그레이션 SQL 생성 유틸
│
└── migration-main/
    ├── requirements.txt
    ├── app/
    │   ├── main.py               # 진입점 (APScheduler 구동)
    │   ├── agent/
    │   │   ├── scheduler.py      # DB 폴링 및 작업 디스패치
    │   │   ├── orchestrator.py   # 마이그레이션 파이프라인 총괄
    │   │   ├── llm_client.py     # LLM SQL 생성 (OpenAI 호환 API)
    │   │   ├── executor.py       # Oracle SQL 실행
    │   │   ├── verifier.py       # 데이터 정합성 검증
    │   │   └── sql_utils.py      # SQL 파싱 유틸
    │   ├── core/
    │   │   ├── db.py             # Oracle 접속 관리 (Thin/Thick 자동 전환)
    │   │   ├── logger.py         # 로거 설정
    │   │   └── exceptions.py     # 커스텀 예외 정의
    │   └── domain/
    │       ├── mapping/          # NEXT_MIG_INFO 모델 및 Repository
    │       └── history/          # NEXT_MIG_LOG Repository
    └── tests/
```

---

## 설치 및 환경 설정

### 1. 의존성 설치

```bash
pip install -r migration-main/requirements.txt
```

**주요 패키지**

| 패키지 | 용도 |
|---|---|
| `oracledb` | Oracle DB 접속 (Thin/Thick 모드) |
| `apscheduler` | 주기적 DB 폴링 스케줄러 |
| `openai` | LLM API 클라이언트 (OpenAI 호환) |
| `python-dotenv` | `.env` 환경변수 로드 |

### 2. 환경변수 설정 (`.env`)

```env
# LLM 설정
OPEN_API_KEY=your_api_key
LLM_MODEL=gemini-2.5-flash-lite
LLM_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/

# 마이그레이션 종류
MIG_KIND=DB_MIG   # DB_MIG 또는 SQL_MIG

# Oracle DB 접속 정보
DB_USER=scott
DB_PASS=tiger
DB_HOST=localhost
DB_PORT=1521
DB_SID=xe

# Oracle Client 경로 (11g 이하 또는 Thick 모드 필요 시)
ORACLE_CLIENT_PATH=C:\oraclexe\app\oracle\product\11.2.0\server\bin
```

### 3. DB 초기화

Oracle SQL*Plus 또는 SQL Developer에서 순서대로 실행합니다.

```sql
-- 1. 메타데이터 테이블 및 기본 매핑 룰 생성
@META_MIGRATION_TO_21C.sql

-- 2. HR 마이그레이션 타겟 테이블 생성
@HR_MIGRATION_TO_21C.sql

-- 3. (선택) 검증용 Simple 케이스 5건 추가
@ADD_SIMPLE_TEST_CASES.sql
```

---

## 실행 방법

```bash
cd migration-main
python -m app.main
```

실행 시 로그 예시:

```
====================================
 스마트 데이터 마이그레이션 에이전트 가동
====================================
APScheduler 가동 시작. (10초 주기로 작업 대기열 스캔)
--- [Scheduler] DB 작업 대상 스캔 ---
처리 대상 작업 발견: 5건
[JOB_START] 대상 작업(map_id=200) 프로세스 시작
[LLM] SQL 생성 완료 (Model: gemini-2.5-flash-lite)
[JOB_PASS] map_id=200 | >>> 마이그레이션 통과 <<<
```

종료: `Ctrl+C`

---

## 마이그레이션 파이프라인

각 작업(`USE_YN='Y'`)은 아래 단계를 순차 실행합니다.

```
1. DDL 조회     → 소스 테이블 컬럼 메타데이터(ALL_TAB_COLUMNS) 읽기
2. SQL 생성     → LLM에 매핑 룰 + DDL 전달 → DDL / MIG_SQL / VERIFY_SQL 반환
3. 테이블 정리  → DROP TABLE IF EXISTS (Clean Retry)
4. DDL 실행    → CREATE TABLE (타겟 테이블 생성)
5. DML 실행    → INSERT INTO ... SELECT ...
6. 검증        → VERIFY_SQL 실행, DIFF=0 확인
7. 결과 기록   → NEXT_MIG_INFO STATUS 업데이트, NEXT_MIG_LOG 기록
```

실패 시 에러 메시지를 LLM에 피드백하여 **최대 3회** 자동 재시도합니다.

---

## DB 테이블 구조

### NEXT_MIG_INFO (매핑 룰)

| 컬럼 | 설명 |
|---|---|
| `MAP_ID` | 매핑 룰 고유 ID (PK) |
| `MAP_TYPE` | `SIMPLE` / `COMPLEX` |
| `FR_TABLE` | 소스 테이블 (JOIN 표현식 포함 가능) |
| `TO_TABLE` | 타겟 테이블명 |
| `USE_YN` | `Y`: 실행 대상 / `N`: 완료·제외 |
| `TARGET_YN` | 작업 대상 여부 |
| `PRIORITY` | 실행 순서 (낮을수록 먼저) |
| `MIG_SQL` | LLM이 생성한 이관 SQL |
| `VERIFY_SQL` | LLM이 생성한 검증 SQL |
| `STATUS` | `PENDING` / `PASS` / `FAIL` |
| `BATCH_CNT` | 총 시도 횟수 |
| `RETRY_COUNT` | 재시도 횟수 |
| `CORRECT_SQL` | 전문가 수정 정답 SQL (LLM 참고용) |

### NEXT_MIG_INFO_DTL (컬럼 매핑)

| 컬럼 | 설명 |
|---|---|
| `MAP_DTL` | 상세 룰 ID (PK) |
| `MAP_ID` | 매핑 룰 ID (FK) |
| `FR_COL` | 소스 컬럼 (표현식 포함 가능) |
| `TO_COL` | 타겟 컬럼명 |

### NEXT_MIG_LOG (실행 이력)

| 컬럼 | 설명 |
|---|---|
| `LOG_ID` | 로그 ID (PK) |
| `MAP_ID` | 매핑 룰 ID |
| `LOG_TYPE` | `INFO` / `ROW_ERROR` / `JOB_FAIL` |
| `LOG_LEVEL` | `INFO` / `WARN` / `ERROR` |
| `STEP_NAME` | `SQL_EXEC` / `VERIFY` |
| `STATUS` | `PASS` / `FAIL` |
| `MESSAGE` | 상세 메시지 |

---

## 등록된 매핑 룰 현황

| MAP_ID | 소스 → 타겟 | 타입 | 상태 |
|---|---|---|---|
| 1 | EMPLOYEES → TGT_EMP | SIMPLE | PASS |
| 3 | EMPLOYEES + JOBS → TGT_EMP_JOB | COMPLEX | PASS |
| 101 | EMPLOYEES + DEPARTMENTS + JOBS → HR_EMP_DEPT_JOB_SNAP | COMPLEX | FAIL |
| 102 | DEPARTMENTS + EMPLOYEES → HR_DEPT_PAYROLL_ROLLUP_F | COMPLEX | PASS |
| 103 | EMPLOYEES + JOB_HISTORY → HR_EMP_CAREER_EVENT_F | COMPLEX | PASS |
| 104 | EMPLOYEES + JOB_HISTORY → HR_EMP_HISTORY_STACKED_F | COMPLEX | PASS |
| 105 | EMPLOYEES + JOBS → HR_EMP_TYPECAST_AUDIT | COMPLEX | PASS |
| 106 | DEPARTMENTS + EMPLOYEES → HR_DEPT_TYPECAST_AUDIT | COMPLEX | PASS |
| 200 | JOBS → TGT_JOBS | SIMPLE | PENDING |
| 201 | DEPARTMENTS → TGT_DEPARTMENTS | SIMPLE | PASS |
| 202 | LOCATIONS → TGT_LOCATIONS | SIMPLE | PENDING |
| 203 | COUNTRIES → TGT_COUNTRIES | SIMPLE | FAIL |
| 204 | REGIONS → TGT_REGIONS | SIMPLE | PASS |

---

## LLM 연동

`LLM_BASE_URL`과 `OPEN_API_KEY`를 변경하면 OpenAI 호환 API라면 무엇이든 교체 가능합니다.

| 항목 | 기본값 |
|---|---|
| 클라이언트 | OpenAI Python SDK |
| 현재 모델 | `gemini-2.5-flash-lite` (Google Generative Language API) |
| 응답 형식 | `json_object` (ddl_sql / migration_sql / verification_sql) |

프롬프트에는 아래 정보가 자동으로 포함됩니다.
- 소스/타겟 테이블명 및 컬럼 매핑 정보
- 소스 테이블 실제 DDL (`ALL_TAB_COLUMNS` 조회 결과)
- Oracle 11.2 XE 제약 사항 (LATERAL JOIN 금지, STANDARD_HASH 금지 등)
- 이전 실패 에러 메시지 및 SQL (재시도 시)
- 전문가 수정 정답 SQL (`CORRECT_SQL`이 있는 경우)

---

## 주의 사항

- 에이전트는 작업 완료 후 해당 룰의 `USE_YN`을 `'N'`으로 변경합니다. 재실행이 필요하면 `USE_YN='Y'`로 수동 변경하세요.
- Oracle 11.2 XE 환경을 타겟으로 하므로 12c 이상 전용 문법(LATERAL, FETCH FIRST 등)은 프롬프트에서 명시적으로 금지됩니다.
- `.env` 파일에 API Key가 포함되어 있으므로 외부 공개 시 주의하세요.
