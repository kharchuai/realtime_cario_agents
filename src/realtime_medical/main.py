import logging
import sys
from src.realtime_medical.config import AppConfig
from src.realtime_medical.streaming.consumer import VitalsConsumer

# Thiết lập logging xuất ra console để dễ dàng theo dõi qua Docker logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RealtimeMedicalApp")

def main():
    logger.info("🚀 Đang khởi động hệ thống Realtime Medical Monitoring...")
    
    try:
        # 1. Load toàn bộ cấu hình từ file YAML và biến môi trường
        config = AppConfig.load_config("configs/config.yaml")
        logger.info(f"Kết nối tới Kafka Broker tại: {config.kafka_broker_url}")
        
        # 2. Khởi tạo Consumer (Bao gồm DB Client, Rule Engine và ML Model bên trong)
        consumer = VitalsConsumer(config)
        
        # 3. Kích hoạt vòng lặp vô hạn để lắng nghe luồng dữ liệu
        logger.info("Hệ thống đã sẵn sàng. Đang chờ dữ liệu...")
        consumer.start_listening()
        
    except FileNotFoundError as e:
        logger.error(f"Lỗi khởi tạo: Không tìm thấy file cấu hình - {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Hệ thống gặp sự cố nghiêm trọng và phải dừng lại: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()