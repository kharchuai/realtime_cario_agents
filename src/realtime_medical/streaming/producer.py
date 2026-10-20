# =========================================================================
# Module: producer.py
#
# Tác dụng: Đóng vai trò "bên gửi" trong luồng dữ liệu realtime — mô phỏng
# thiết bị/cảm biến đo sinh hiệu bệnh nhân, đóng gói dữ liệu thành JSON và
# đẩy vào Kafka topic "patient_vitals_stream" để consumer bên kia xử lý.
#
# - VitalsProducer.__init__   : Kết nối tới Kafka broker (địa chỉ lấy từ
#                                AppConfig.kafka_broker_url).
# - send_vital_sign()         : Nhận dữ liệu sinh hiệu (dict) của 1 bệnh
#                                nhân, gắn thêm patient_id + timestamp
#                                (produced_at) vào 1 payload MỚI (không
#                                sửa dict gốc truyền vào), rồi gửi lên Kafka.
#                                Dùng patient_id làm key để đảm bảo dữ liệu
#                                cùng 1 bệnh nhân giữ đúng thứ tự thời gian.
# - delivery_report()         : Callback được Kafka tự gọi để báo gửi
#                                thành công/thất bại từng message.
# - flush()                   : Đảm bảo toàn bộ message còn trong buffer
#                                được gửi hết trước khi tắt chương trình,
#                                tránh mất dữ liệu khi shutdown.
# =========================================================================
import time
import json
import logging
from confluent_kafka import Producer
from src.realtime_medical.config import AppConfig
from src.realtime_medical.utils.benchmark_hooks import make_metrics, record_id
logger = logging.getLogger(__name__)

class VitalsProducer:
    def __init__(self, config: AppConfig):
        # Configuration for connecting to the Kafka Broker
        conf = {
            'bootstrap.servers': config.kafka_broker_url,
            'client.id': 'medical-sensor-producer',
            'enable.idempotence': True,
            'acks': "all",
        }
        self.producer = Producer(conf)
        self.topic = config.kafka_topic_vitals  # Match the configuration in the .env file
        self.delivery_errors = []
        self.metrics = make_metrics("producer")

    def delivery_report(self, err, msg):
        """ The callback is invoked when Kafka successfully receives a message or fails to do so"""
        if err is not None:
            self.delivery_errors.append(str(err))
            logger.error(f"Failed to send data: {err}")
        else:
            logger.info(
                "KAFKA_DELIVERED topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
            
            
    def send_vital_sign(self,surgery_id:int, patient_id: str, vital_data: dict):
        """ Function to package data as JSON and push it to Kafka"""
        payload = {
            **vital_data,
            "surgery_id": surgery_id,
            "patient_id": patient_id,
            "produced_at": time.time(),   # dùng underscore cho nhất quán style
        }
        rid = None
        if self.metrics.enabled:
            rid = record_id(self.metrics.run_id, surgery_id, vital_data["recorded_at"])
            self.metrics.trace("producer_attempt", rid)

        def on_delivery(err, msg):
            """Gắn Kafka ACK/error với ID reading được giữ trong closure.

            Notes:
                Giữ delivery_report cũ để flush vẫn báo delivery_errors.
                send_vital_sign trả True chỉ chứng minh client enqueue, chưa phải ACK.
                Khi tắt đo, vẫn xử lý kết quả gửi nhưng không ghi trace.
            """
            self.delivery_report(err, msg)
            if not self.metrics.enabled:
                return
            if err is not None:
                self.metrics.trace(
                    "kafka_delivery_failed", rid, error_type = type(err).__name__
                )
            else:
                self.metrics.trace(
                    "kafka_acked", rid, topic = msg.topic(), partition = msg.partition(), offset = msg.offset(),
                )

        for attempt in range(2):
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=str(surgery_id),
                    value=json.dumps(payload).encode('utf-8'),
                    callback= on_delivery
                )
                self.producer.poll(0)
                return True

            except BufferError:
                if attempt == 0:
                    self.producer.poll(1)
                    continue
                logger.exception("Kafka producer queue remains full")
                return False
            except Exception as e:
                logger.exception(f"Fail when send data for surgery_id {surgery_id}, patient {patient_id}:{e}")
                return False

    def flush(self, timeout: float = 30.0):
        remaining = self.producer.flush(timeout)
        if remaining:
            raise RuntimeError(f"{remaining} Kafka message(s) remained after flush")

        if self.delivery_errors:
            raise RuntimeError("Kafka delivery failed: " + "; ".join(self.delivery_errors[:5]))
    def close(self, timeout = 30.0):
        """Flush Kafka callbacks rồi đóng collector ở cuối vòng đời producer.

        Args:
            timeout: Số giây tối đa chờ flush hiện có.

        Notes:
            Chỉ gọi sau khi replay threads dừng gửi.
            Có thể flush nhiều lần giữa run; close chỉ dùng lúc kết thúc.
        """
        try:
            self.flush(timeout)
        finally:
            self.metrics.close()
