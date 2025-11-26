import time
import json
import random
from datetime import datetime


CLIENTS = [
    {"client_id": "C001", "name": "Zurich University Hospital", "type": "HOSPITAL"},
    {"client_id": "C002", "name": "ETH Zurich Main Building", "type": "UNIVERSITY"},
    {"client_id": "C003", "name": "FELFEL HQ Zurich Office", "type": "OFFICE"},
]


def generate_fridge_event() -> dict:
    """Generate one fake IoT event for a fridge."""
    client = random.choice(CLIENTS)

    return {
        "event_time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "fridge_id": f"FRIDGE_{random.randint(1, 50):03d}",
        "client_id": client["client_id"],
        "location_type": client["type"],
        "occupancy_index": round(random.uniform(0.1, 1.5), 2),  # people density indicator
        "stock_level": round(random.uniform(0, 1), 2),          # 0 = empty, 1 = full
        "fridge_temperature": round(random.uniform(1.0, 7.0), 1),
        "door_opens": random.randint(0, 20),
        "is_low_stock": 1 if random.random() < 0.2 else 0,
    }


def run_simulator(send_fn, interval_seconds: float = 2.0):
    """
    Continuously generate events and send them using the provided send_fn.

    send_fn: function(event_dict) -> None
    """
    print(f"[SIM] Starting fridge IoT simulator with interval = {interval_seconds}s")
    while True:
        event = generate_fridge_event()
        send_fn(event)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    # Example usage for quick local test: just print events
    def _print(e):
        print(json.dumps(e))

    run_simulator(_print, interval_seconds=1.0)
