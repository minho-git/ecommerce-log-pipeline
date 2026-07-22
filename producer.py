"""
Kafka Producer

목적:
- data/raw/*.json에 저장된 로그를 읽어서 Kafka topic("web-logs")으로 전송.
- 실제로는 웹 서버가 유저 행동이 발생할 때마다 실시간으로 로그를 쏘겠지만,
  여기서는 이미 만들어둔 파일을 읽어서 "재생(replay)"하는 방식으로 흉내낸다.

메시지 키 설계:
- key=user_id로 지정. 같은 유저의 로그는 항상 같은 파티션으로 가게 되어
  (파티션이 여러 개로 늘어나도) 유저별 이벤트 순서가 보장된다.
- user_id가 없는 로그(비로그인 유저)는 key 없이 전송 -> 라운드로빈으로 분산.
"""

import json
import time
from pathlib import Path

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "web-logs"
RAW_DIR = Path(__file__).parent / "data" / "raw"
SEND_DELAY_SECONDS = 0.01  # 실시간 스트리밍처럼 보이게 주는 약간의 딜레이


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )


def main():
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        print(f"전송할 로그 파일이 없습니다: {RAW_DIR}")
        return

    producer = build_producer()
    total_sent = 0

    for file_path in files:
        with open(file_path, encoding="utf-8") as f:
            logs = json.load(f)

        for log in logs:
            producer.send(TOPIC, key=log.get("user_id"), value=log)
            total_sent += 1
            time.sleep(SEND_DELAY_SECONDS)

        print(f"전송 완료: {file_path.name} ({len(logs)}건)")

    producer.flush()
    producer.close()
    print(f"총 {total_sent}건 전송 완료 -> topic '{TOPIC}'")


if __name__ == "__main__":
    main()
