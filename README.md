# 이커머스 유저 행동 로그 파이프라인 (Log Pipeline Project)

## 프로젝트 목적
데이터 엔지니어 인턴 준비를 위한 개인 프로젝트.
실무 데이터 파이프라인 구조(Kafka → Spark → Medallion Architecture → Airflow)를
로컬 환경(Docker Compose)에서 최대한 비슷하게 재현하는 것이 목표.

단순히 각 기술을 따로 써보는 게 아니라, **왜 이런 구조로 설계했는지**를
설명할 수 있는 것이 이 프로젝트의 핵심 가치임.

---

## 다루는 데이터: 이커머스 웹사이트 유저 행동 로그 (Clickstream)

유저가 웹사이트에서 페이지를 보고, 클릭하고, 장바구니에 담고, 구매하는
행동을 기록한 로그. 실무에서 데이터 엔지니어가 가장 흔히 다루는 로그 유형.

### 로그 스키마 (raw)
```json
{
  "log_id": "string (UUID)",       // 유니크 키, 중복 감지용
  "user_id": "string | null",      // 비로그인 유저는 null 가능
  "timestamp": "ISO8601 string",
  "page": "string",                // /, /products, /cart, /checkout 등
  "action": "string",              // page_view, click, add_to_cart, purchase
  "session_id": "string"
}
```

### 의도적으로 반영한 실무형 데이터 문제
- `user_id` 일부 결측 (비로그인 유저 시나리오)
- `page` 값 일부 빈 문자열 (프론트 버그 재현)
- 동일 `log_id` 중복 발생 (네트워크 재전송/재시도 재현)
→ 이 문제들을 Silver 레이어에서 어떻게 감지·처리하는지가
   "정합성", "데이터 유실 방지"를 보여주는 핵심 파트임.

---

## 목표 아키텍처

```
[로그 생성기 (Python)] - 완료
    ↓
[Kafka Producer] → 로그를 Kafka topic으로 전송
    ↓
[Kafka 클러스터 (Docker Compose, 단일 브로커로 충분)]
    ↓
[Kafka Consumer / Spark Structured Streaming] → Kafka에서 읽어서 저장
    ↓
[Bronze 레이어] 원본 그대로 저장 (Parquet 포맷, 날짜별 파티셔닝)
    ↓
[Silver 레이어] 정제
    - 중복 log_id 제거
    - user_id 결측치 처리 (예: "anonymous"로 대체 또는 별도 플래그)
    - page 빈 값 필터링/처리
    - 데이터 품질 체크: 오늘 로그 건수가 최근 평균 대비 급감하면 경고 로그 출력
    ↓
[Gold 레이어] 비즈니스 지표 집계
    - 일별 방문자 수 (DAU)
    - 페이지별 조회수 Top N
    - 장바구니 담기 → 구매 전환율
    - 유저별 세션 수
    ↓
[Airflow] 위 전체 파이프라인을 매일 자동 실행되도록 DAG로 스케줄링
    (Bronze 적재 → Silver 정제 → Gold 집계 순서로 task dependency 구성)
```

---

## 기술 스택
- Python 3.11+
- Apache Kafka (Docker Compose로 로컬 실행, 단일 브로커)
- PySpark (Structured Streaming 또는 배치 처리 - 상황에 맞게 선택)
- Parquet (저장 포맷)
- Apache Airflow (파이프라인 오케스트레이션)
- Docker / Docker Compose

---

## 현재까지 진행 상황
- [x] 로그 생성기 (`generate_logs.py`) - 완료. 7일치 가짜 로그를 
      `data/raw/YYYY-MM-DD.json` 형태로 생성함. 의도적으로 결측치/중복 포함.
- [ ] Docker Compose로 Kafka 환경 구성
- [ ] Kafka Producer: 생성된 로그를 Kafka topic(`web-logs`)으로 전송
- [ ] Kafka Consumer: topic에서 읽어서 Bronze(Parquet)로 저장
- [ ] Silver 레이어: 정제 + 데이터 품질 체크 로직
- [ ] Gold 레이어: 집계 로직
- [ ] Airflow DAG 작성
- [ ] README에 트레이드오프/설계 의도 문서화 (면접 대비용)

---

## 클로드 코드에게: 다음으로 할 일

1. `generate_logs.py`는 이미 완성되어 있음 (그대로 사용, 필요시 리팩토링 제안 가능)
2. `docker-compose.yml` 작성: Kafka(단일 브로커, KRaft 모드로 Zookeeper 없이 
   최신 방식 권장) + 필요시 Kafka UI(Kafdrop 등) 포함해서 눈으로 확인 가능하게
3. `producer.py`: `data/raw/*.json`의 로그를 한 줄씩(또는 배치로) Kafka topic 
   `web-logs`로 전송하는 스크립트. 실시간처럼 보이게 약간의 delay를 줘도 좋음
4. `consumer.py`: `web-logs` topic을 구독해서 Bronze 레이어에 Parquet로 저장. 
   날짜별 파티셔닝 적용 (`data/bronze/date=YYYY-MM-DD/*.parquet`)
5. `silver_transform.py`: Bronze → Silver 정제 로직
   - 중복 log_id 제거 (dedup)
   - user_id 결측 처리
   - page 빈 값 처리
   - 품질 체크: 예상 건수 대비 실제 건수 비교해서 임계치 이하면 경고 로그
6. `gold_aggregate.py`: Silver → Gold 집계 로직 (DAU, 페이지별 조회수, 전환율 등)
7. `dags/log_pipeline_dag.py`: Airflow DAG. Kafka consume → Bronze → Silver → 
   Gold 순서로 task chain 구성. 실패 시 재시도/알림 설정 포함
8. 각 단계 작성 후 실행 방법을 README에 추가 (예: `docker-compose up -d` 등)
9. 최종적으로 프로젝트 루트에 아키텍처 다이어그램(mermaid 등)과 
   "설계 시 고민한 트레이드오프" 섹션을 README에 추가해줄 것
   (예: "왜 ETL이 아니라 ELT/메달리온을 선택했는지", 
   "왜 Kafka를 도입했는지 - 배치로만 처리했을 때와 비교해서 장단점")

## 원칙
- 각 단계는 독립적으로 실행 테스트 가능해야 함 (전체를 한 번에 안 돌려도 
  Bronze만, Silver만 따로 테스트 가능하게)
- 코드에 주석으로 "왜 이렇게 설계했는지" 설명을 남길 것 (면접 스토리텔링용)
- 과도한 엔지니어링 지양: 로컬 개인 프로젝트 수준에 맞는 간결한 구현 우선
