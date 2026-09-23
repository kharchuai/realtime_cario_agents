# =========================================================================
# Module: consumer.py
#
# Tác dụng: Đóng vai trò "bên nhận" trong luồng dữ liệu realtime — lắng
# nghe Kafka topic "patient_vitals_stream", với mỗi message nhận được sẽ:
#   1. Lưu dữ liệu sinh hiệu vào database (bảng vitals_stream).
#   2. Đánh giá mức độ nguy hiểm bằng 2 cách kết hợp:
#      - Rule-based (RuleEngine)   : so với ngưỡng trong triage_rules
#        (config.yaml) như nhịp tim cao/thấp, spo2 critical...
#      - Machine learning (MLTriageModel): dự đoán điểm rủi ro dựa trên
#        model đã train (đường dẫn ở config.model).
#   3. Nếu phát hiện nguy cơ (CRITICAL hoặc ML score vượt ngưỡng) -> ghi
#      log cảnh báo và lưu vào bảng alert để bác sĩ trực tra cứu.
#
# VitalsConsumer.__init__   : Kết nối Kafka (consumer group), khởi tạo
#                              DB client, RuleEngine và MLTriageModel.
# start_listening()         : Vòng lặp chính, chạy liên tục cho tới khi
#                              nhận Ctrl+C (KeyboardInterrupt) thì dừng.
# =========================================================================
import json
import logging
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from src.realtime_medical.config import AppConfig
from src.realtime_medical.db.client import DataBaseClient
from src.realtime_medical.streaming.windowing import FeatureBuilder, StaticFeatureStore, WindowAggregator
from src.realtime_medical.triage.rules import RuleEngine
from src.realtime_medical.triage.trigger_model import MLTriageModel
logger = logging.getLogger(__name__)

class VitalsConsumer:
    def __init__(self, config: AppConfig):
        # Group consumer (Consumer Group) to avoid duplicate data reading 
        conf ={
            'bootstrap.servers':config.kafka_broker_url,
            'group.id': 'medical-triage-group',
            'auto.offset.reset': 'latest' #It's realtime so only latest data matter   
        }
        self.consumer = Consumer(conf)
        # This topic name matches the environment variable configuration
        self.topic = "patient_vitals_stream"
        self.consumer.subscribe([self.topic])
        
        # Start DB  Client
        self.db_client= DataBaseClient(config)
        self.rule_engine = RuleEngine(config.triage_rules)
        self.ml_model = MLTriageModel(config.model)  
        self.feature_builder = FeatureBuilder(
            WindowAggregator(config.streaming), StaticFeatureStore(self.db_client)
        )
        
        self.batch_size = config.streaming.batch_size
        self.flush_interval = config.streaming.window_size_seconds
        
        self._vitals_buffer =  []
        self._alerts_buffer = []
        self._last_flush_time = time.time()

    def _should_flush(self) -> bool:
        buffer_full = len(self._vitals_buffer) >= self.batch_size
        time_elapse = (time.time() - self._last_flush_time) >= self.flush_interval
        has_pending_data = bool(self._vitals_buffer or self._alerts_buffer)
        return has_pending_data and( buffer_full or time_elapse)
    
    
    def _flush_vitals(self) -> None:
        if not self._vitals_buffer:
            return
        try:
            self.db_client.insert_vitals_batch(self._vitals_buffer)
        except Exception as e:
            logger.error(
                f"Batch insert vitals fails ({len(self._vitals_buffer)} row)"
                f"Fallback command prompts you to insert line to isolate the faulty line: {e}"
            )
            for surgery_id, vitals in self._vitals_buffer:
                try:
                    self.db_client.insert_vitals(surgery_id, vitals)
                except  Exception as row_err:
                    logger.error(f"Skipping 1 invalid vitals row (surgery_id = {surgery_id}): {row_err}")
        finally:
            self._vitals_buffer.clear()
            
    def _flush_alerts(self) -> None:
        if not self._alerts_buffer:
            return
        try:
            self.db_client.insert_alerts_batch(self._alerts_buffer)
        except Exception as e:
            logger.error(
                f"Batch insert of alert failed ({len(self._alerts_buffer)} rows)"
                f"falling back to row-by-row insertion: {e}"
            )
            for alert in self._alerts_buffer:
                try:
                    self.db_client.insert_alert(
                        alert["surgery_id"],
                        alert["recorded_at"],
                        alert["risk_level"],
                        alert["model_score"],
                        alert["alert_reason"],
                    )
                except Exception as row_err:
                    logger.error(f"Skipping 1 invalid alert row (surgery_id = {alert['surgery_id']}): {row_err}")
                    
        finally:
            self._alerts_buffer.clear()
            
    def _flush(self) -> None:
        self._flush_vitals()
        self._flush_alerts()
        self._last_flush_time = time.time()
            
        
    def start_listening(self):
        logger.info(f"Consumer is listening to topic: {self.topic} ..." 
                    f"(batch_size = {self.batch_size}, flush_interval = {self.flush_interval})")
        try:
            while True:
                # Poll data each second
                msg = self.consumer.poll(timeout = 1)
                
                if msg is None:
                    if self._should_flush():
                        self._flush()
                    continue
                
                if msg.error():
                    if msg.error().code() ==  KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka Error Consumer:  {msg.error()}")
                        break
                    
                # Decode JSON data from a byte string
                try:
                    raw_data = msg.value().decode('utf-8')
                    vitals_data = json.loads(raw_data)
                
                    # Extract patient_id from the vitals dictionary
                    surgery_id = vitals_data.pop("surgery_id", None)
                    patient_id = vitals_data.pop("patient_id", "UNKNOWN")
                    
                    if surgery_id is None:
                        logger.error(f"Message missing surgery_id, skipping (patient = {patient_id})")
                        continue
                    
                    if vitals_data.get("recorded_at") is None:
                        logger.error(f"Message  missing recorded_at, ignore (surgery = {surgery_id})")
                        continue  
                    
                    logger.info(f"Received data steam from surgery {surgery_id} (patient {patient_id})")
                    
                    self._vitals_buffer.append((surgery_id, vitals_data))

                    # Rules use each reading; ML uses the configured streaming window.
                    risk_level, rule_reasons = self.rule_engine.evaluate(vitals_data)
                    ml_score = None
                    try:
                        recorded_at = datetime.fromisoformat(
                            vitals_data["recorded_at"].replace("Z", "+00:00")
                        ).timestamp()
                        features = self.feature_builder.process_reading(
                            surgery_id, vitals_data, timestamp=recorded_at
                        )
                        if features is not None:
                            ml_score = self.ml_model.predict_risk(features)
                    except Exception:
                        logger.exception("ML feature extraction or prediction failed for surgery_id=%s", surgery_id)

                    # Decided whether or not to create an alert
                    ml_alert = ml_score is not None and self.ml_model.is_alert_required(ml_score)
                    if risk_level == "CRITICAL" or ml_alert:
                        alert_reason = " | ".join(rule_reasons)
                        if ml_alert:
                            alert_reason = " | ".join(filter(None, [alert_reason, f"ML Score warning: {ml_score:.2f}"]))

                        logger.warning(f"Risk Detected [surgery_id = {surgery_id}, patient_id = {patient_id}]:{alert_reason}]")

                        self._alerts_buffer.append({
                            "surgery_id": surgery_id,
                            "recorded_at": vitals_data["recorded_at"],
                            "risk_level": risk_level if risk_level == "CRITICAL" else "MEDIUM",
                            "model_score": ml_score,
                            "alert_reason": alert_reason,
                        })
                        
                    if self._should_flush():
                        self._flush()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue    
        except KeyboardInterrupt:
            logger.info("Receive command to stop the consumer")
        finally:
            self._flush()
            self.consumer.close()
            self.db_client.close()

if __name__ == "__main__":
    # Independent consumer test run
    config = AppConfig.load_config()
    consumer = VitalsConsumer(config)
    consumer.start_listening()
