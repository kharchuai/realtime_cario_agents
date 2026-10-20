import logging
import sys
import os
import signal
from pathlib import Path

from src.realtime_medical.config import AppConfig
from src.realtime_medical.streaming.consumer import VitalsConsumer
from src.realtime_medical.utils.fault_injection import install_alert_ack_lose

# Ghi console và file ở cả chế độ thường lẫn chế độ đo metrics.
log_dir = Path(__file__).resolve().parents[2] / "logs" / "pipeline"
log_dir.mkdir(parents=True, exist_ok=True)
handlers = [
    logging.StreamHandler(sys.stdout),
    logging.FileHandler(log_dir / "consumer.log", mode="a", encoding="utf-8"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
    handlers = handlers,
    force = True
)
logger = logging.getLogger("RealtimeMedicalApp")

def handle_shutdown(signum, frame):
    """Đưa SIGTERM của Docker qua cleanup sẵn có như khi nhấn Ctrl+C."""
    raise KeyboardInterrupt

def main():
    signal.signal(signal.SIGTERM, handle_shutdown)
    logger.info("Starting the Realtime Medical Monitoring system...")
    
    try:
        # 1. Load toàn bộ cấu hình từ file YAML và biến môi trường
        config = AppConfig.load_config("configs/config.yaml")
        logger.info(f"Connecting to Kafka broker at: {config.kafka_broker_url}")
        
        # 2. Khởi tạo Consumer (Bao gồm DB Client, Rule Engine và ML Model bên trong)
        consumer = VitalsConsumer(config)

        # 3. Kích hoạt vòng lặp vô hạn để lắng nghe luồng dữ liệu
        restore_ack_fault =  None
        if consumer.metrics.enabled and os.environ.get("BENCH_ALERT_ACK_LOSS") == "1":
            restore_ack_fault = install_alert_ack_lose(consumer.db_client)
        try:
            logger.info("System is ready. Waiting for data...")
            consumer.start_listening()
        finally:
            if restore_ack_fault is not None:
                restore_ack_fault()
        


    except FileNotFoundError as e:
        logger.error(f"Initialization error: Configuration file not found - {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"System encountered a critical error and must stop: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
