"""
1주차: 웹 로그 생성기 (Log Generator)

목적:
- 실제 회사 로그 데이터는 구하기 어려우니, 가짜 웹 방문 로그를 직접 생성해서
  파이프라인의 "소스 데이터"로 사용한다.
- 실제 로그처럼 보이도록 일부러 지저분한 데이터(결측치, 중복, 이상치)도 섞는다.
  -> 나중에 Silver 레이어에서 이걸 정제하는 연습을 하기 위함.

출력:
- data/raw/YYYY-MM-DD.json (하루 단위 로그 파일)
  실무에서도 보통 날짜별로 로그를 파티셔닝해서 저장한다 (파티셔닝 개념 복습!)
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ---- 설정값 ----
PAGES = ["/", "/products", "/products/123", "/cart", "/checkout", "/about", "/login"]
ACTIONS = ["page_view", "click", "add_to_cart", "purchase"]
NUM_USERS = 50
NUM_LOGS_PER_DAY = 500

OUTPUT_DIR = Path(__file__).parent / "data" / "raw"


def generate_one_log(base_time: datetime) -> dict:
    """로그 한 건을 생성한다. 실제 로그처럼 일부러 결측치/이상치를 섞는다."""

    log = {
        # log_id: 유실/중복 감지를 위한 유니크 키 (정합성 체크의 핵심 포인트)
        "log_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, NUM_USERS)}",
        "timestamp": (base_time + timedelta(seconds=random.randint(0, 86399))).isoformat(),
        "page": random.choice(PAGES),
        "action": random.choice(ACTIONS),
        "session_id": f"session_{random.randint(1, NUM_USERS * 3)}",
    }

    # 일부러 지저분하게 만들기 (실제 로그의 현실을 반영)
    # 1) 가끔 user_id 누락 (비로그인 유저 등 실무에서 흔함)
    if random.random() < 0.03:
        log["user_id"] = None

    # 2) 가끔 페이지 값이 이상하게 들어감 (프론트 버그로 잘못된 값이 실릴 수 있음)
    if random.random() < 0.01:
        log["page"] = ""

    return log


def generate_day_logs(date: datetime) -> list[dict]:
    logs = [generate_one_log(date) for _ in range(NUM_LOGS_PER_DAY)]

    # 3) 일부러 중복 로그 몇 개 섞기 (같은 log_id로 두 번 들어오는 상황 재현)
    #    -> 실무에서 네트워크 재전송, 클라이언트 재시도 등으로 흔히 발생하는 유실/중복 케이스
    duplicates = random.sample(logs, k=5)
    logs.extend(duplicates)

    random.shuffle(logs)
    return logs


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 최근 7일치 로그를 하루 단위 파일로 생성 (날짜별 파티셔닝 연습)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    for i in range(7):
        date = today - timedelta(days=i)
        logs = generate_day_logs(date)

        file_path = OUTPUT_DIR / f"{date.strftime('%Y-%m-%d')}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)

        print(f"생성 완료: {file_path} ({len(logs)}건)")


if __name__ == "__main__":
    main()
