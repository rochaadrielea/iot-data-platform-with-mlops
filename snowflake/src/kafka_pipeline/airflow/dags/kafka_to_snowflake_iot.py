import json
from kafka import KafkaProducer

from pathlib import Path
import sys

print("[BOOT] Starting Kafka IoT producer script...")

# ---------------------------------------------------------
# 1) Fix path to common/iot_simulator
# ---------------------------------------------------------
ROOT = Path(__file__).resolve().parents[3]
# For you, this should be:
# C:\Users\adrie\Documents\Projects_Data\DP203\iot-felfel-data-platform
print(f"[BOOT] Detected ROOT = {ROOT}")

COMMON_PATH = ROOT / "common" / "iot_simulator"
print(f"[BOOT] Expecting simulator module in: {COMMON_PATH}")

sys.path.append(str(COMMON_PATH))

try:
    from fridge_iot_simulator import run_simulator  # type: ignore
    print("[BOOT] Imported fridge_iot_simulator successfully.")
except Exception as e:
    print("[ERROR] Could not import fridge_iot_simulator:", repr(e))
    raise


KAFKA_BROKER = "localhost:9092"   # outside docker
TOPIC_NAME = "fridge_iot"


def send_to_kafka(event: dict):
    """Send event to Kafka topic."""
    global producer

    msg_bytes = json.dumps(event).encode("utf-8")
    producer.send(TOPIC_NAME, msg_bytes)
    producer.flush()
    print(f"[KAFKA] Sent event to {TOPIC_NAME}: {event['fridge_id']} @ {event['event_time']}")


if __name__ == "__main__":
    print(f"[BOOT] Creating KafkaProducer to {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("[BOOT] KafkaProducer created.")
    except Exception as e:
        print("[ERROR] Could not create KafkaProducer:", repr(e))
        raise

    print("[BOOT] Starting simulator loop...")
    run_simulator(send_to_kafka, interval_seconds=2.0)

