# =========================================================================
# Module: consumer.py
#
# Tác dụng: Đóng vai trò "bên nhận" trong luồng dữ liệu realtime — lắng
# nghe Kafka topic "patient_vitals_stream", với mỗi message nhận được sẽ:
#
#   - Khi có kết quả ML mới hoặc rules trả CRITICAL, tạo RiskEvent và gửi
#  tới agent worker chạy nền.
#   - Agent xuất giải thích ra console; việc ghi cảnh báo hiện có vẫn do
#  consumer quản lý.
#
#  1. Lưu dữ liệu sinh hiệu vào database (bảng vitals_stream).
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
import os

from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from sqlalchemy.exc import DataError, IntegrityError

from src.realtime_medical.config import AppConfig
from src.realtime_medical.db.client import DataBaseClient
from src.realtime_medical.streaming.windowing import FeatureBuilder, StaticFeatureStore, WindowAggregator
from src.realtime_medical.triage.rules import RuleEngine
from src.realtime_medical.triage.trigger_model import MLTriageModel

from src.realtime_medical.agent import AgentPolicy, RiskEvent
from src.realtime_medical.agent.runtime import AgentRuntime, AgentRuntimeError

from src.realtime_medical.utils.benchmark_hooks import (logical_alert_id, make_metrics, mark_vitals_persisted, mark_alerts_persisted, record_id)
from src.realtime_medical.utils.signal_monitor import SignalMonitor

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
        self.topic = config.kafka_topic_vitals
        self.consumer.subscribe([self.topic], on_assign=self._on_assign)
        
        # Start DB  Client
        self.db_client= DataBaseClient(config)
        self.rule_engine = RuleEngine(config.triage_rules)
        self.ml_model = MLTriageModel(config.model)  
        self.feature_builder = FeatureBuilder(
            WindowAggregator(config.streaming), StaticFeatureStore(self.db_client)
        )
        
        self.batch_size = config.streaming.batch_size
        self.flush_interval = config.streaming.window_size_seconds
        self.retry_interval = 5.0

        self._vitals_buffer =  []
        self._alerts_buffer = []

        self._last_flush_time = time.monotonic()
        self._next_retry_time = 0.0
        self._retry_pending = False

        self.agent_runtime = AgentRuntime(
            policy =  AgentPolicy(
                watch_enter = 0.35,
                watch_exit = 0.25,
                high_enter = 0.65,
                high_exit = 0.55,
                horizon_seconds = self.ml_model.prediction_horizon_seconds
            ),
        )

        self.config = config
        self.metrics = make_metrics("consumer")
        self.signal_monitor = None
        if self.metrics.enabled:
            self.signal_monitor = SignalMonitor(self.metrics, config)
            fields = [
                f.strip() for f in os.environ.get(
                    "BENCH_SIGNAL_FIELDS",
                    os.environ.get("BENCH_SIGNAL_FIELD", "hr,spo2"),
                ).split(",") if f.strip()
            ]
            for value in os.environ.get("BENCH_CASES", "").split(","):
                if value.strip():
                    self.signal_monitor.register(int(value.strip()), fields)

    def _on_assign(self, consumer, partitions):
        """Báo Kafka đã phân partition để runner không phát trước lúc sẵn sàng."""
        logger.info("KAFKA_ASSIGNED partitions=%s", len(partitions))
        if self.metrics.enabled:
            self.metrics.trace("consumer_assigned", "consumer", partitions=len(partitions))

    def _should_flush(self) -> bool:
        """Quyết định khi nào ghi dữ liệu đang nằm trong buffer.

        Flush khi đủ batch hoặc hết thời gian gom dữ liệu. Nếu lượt ghi
        trước thất bại, chỉ thử lại sau retry_interval để tránh gây tải DB.
        """

        has_pending_data = bool(self._vitals_buffer or self._alerts_buffer)
        if not has_pending_data:
            return False
        
        now = time.monotonic()
        if self._retry_pending:
            return now >= self._next_retry_time
        
        buffer_full = (
            len(self._vitals_buffer) >= self.batch_size
            or len(self._alerts_buffer) >= self.batch_size
        )
        interval_elapsed = (
            now - self._last_flush_time >= self.flush_interval
        )

        return buffer_full or interval_elapsed
    
    
    def _flush_vitals(self) -> None:
        """Quyết định khi nào ghi dữ liệu đang nằm trong buffer.

        Flush khi đủ batch hoặc hết thời gian gom dữ liệu. Nếu lượt ghi
        trước thất bại, chỉ thử lại sau retry_interval để tránh gây tải DB.
        """
        if not self._vitals_buffer:
            return
        
        rows = list(self._vitals_buffer)

        try:
            inserted = self.db_client.insert_vitals_batch(rows)
            if self.metrics.enabled:
                ack_ns = time.time_ns()
                mark_vitals_persisted(self.metrics, self.config, rows, ack_ns)
        except (ValueError, DataError, IntegrityError):
            logger.exception(
                "VITALS_BATCH_DATA_ERROR rows=%s; retrying individual rows",
                len(rows),
            )
            
        
        else:
            # Không có exception: batch đã được ghi hoặc bản ghi đã tồn tại.
            # inserted có thể bằng 0 nếu tất cả đều trùng khóa.
            self._vitals_buffer.clear()

            logger.info(
                "VITALS_FLUSH_OK handled=%s newly_inserted=%s",
                len(rows),
                inserted,
            )
            return 
        
        pending = []
        for surgery_id, vitals in rows:
            try:
                self.db_client.insert_vitals_batch([(surgery_id, vitals)])
                if self.metrics.enabled:
                    ack_ns = time.time_ns()
                    mark_vitals_persisted(self.metrics, self.config, [(surgery_id, vitals)], ack_ns)

            
            except Exception:
                pending.append((surgery_id, vitals))

                logger.exception(
                    "VITAL_ROW_WRITE_FAILED surgery_id=%s "
                    "recorded_at=%s; retaining for retry",
                    surgery_id,
                    vitals.get("recorded_at"),
                )

        self._vitals_buffer = pending

        logger.info(
            "VITALS_FLUSH_PARTIAL handled=%s pending=%s",
            len(rows) - len(pending),
            len(pending)
        )            
            
    def _flush_alerts(self) -> None:
        """Ghi cảnh báo sau khi sinh hiệu tương ứng đã được lưu.

        Những cảnh báo có sinh hiệu còn trong buffer được giữ lại, tránh
        vi phạm khóa ngoại. Các cảnh báo ghi thất bại cũng được giữ để thử sau.

        API cảnh báo hiện chưa chống trùng khi thử lại sau một lỗi không rõ
        transaction đã commit hay chưa; cần bổ sung idempotency ở tầng DB
        để giải quyết trường hợp đó.
        """
        if not self._alerts_buffer:
            return

        blocked_keys = {
            (surgery_id, vitals.get("recorded_at"))
            for surgery_id, vitals in self._vitals_buffer
        }

        ready = []
        waiting = [] 
        for alert in self._alerts_buffer:
            key = (
                alert["surgery_id"],
                alert["recorded_at"],
            )
        
            if key in blocked_keys:
                waiting.append(alert)
            else:
                ready.append(alert)
        
        if not ready:
            logger.warning(
                f"ALERTS_WAITiNG_FOR_VITALS pending {len(waiting)}"
            )
            return
        try:
            self.db_client.insert_alerts_batch(ready)
            if self.metrics.enabled:
                ack_ns = time.time_ns()
                mark_alerts_persisted(self.metrics, ready, ack_ns)
        except (ValueError, DataError, IntegrityError):
            logger.exception(f"ALERTS_BATCH_DATA_ERROR rows = {len(ready)}; retrying individual rows")
        
        except Exception:
            logger.exception(
                f"ALERTS_BATCH_WRITE_FAILD rows = {len(ready)}; retaining for retry"
            )
            return
        
        else:
            self._alerts_buffer = waiting

            logger.info(f"ALERTS_FLUSH_OK saved = {len(ready)} waiting = {len(waiting)}")
            return
        
        failed = []

        for alert in ready:
            try:
                self.db_client.insert_alert(
                alert["surgery_id"],
                alert["recorded_at"],
                alert["risk_level"],
                alert["model_score"],
                alert["alert_reason"],
                )
                if self.metrics.enabled:
                    ack_ns = time.time_ns()
                    mark_alerts_persisted(self.metrics, [alert], ack_ns)
            except Exception: 
                failed.append(alert)

                logger.exception(
                    f"ALERT_ROWS_WRITE_FAILED surgery_id = {alert['surgery_id']} "
                    f"recorded_at = {alert['recorded_at']}; retaining for retry")
        self._alerts_buffer = waiting + failed

        logger.info(f"ALERTS_FLUSH_PARTIAL saved = {len(ready) - len(failed)} failed = {len(failed)} waiting = {len(waiting)}")

            
    def _flush(self) -> None:
        """Ghi sinh hiệu trước cảnh báo và lên lịch thử lại khi còn dữ liệu.

        Một lượt flush có thể hoàn thành một phần. Dữ liệu còn lại được giữ
        trong RAM và chỉ được thử lại sau retry_interval.
        """
        self._flush_vitals()
        self._flush_alerts()
        
        now = time.monotonic()
        self._last_flush_time = now

        self._retry_pending = bool(
            self._vitals_buffer or self._alerts_buffer
        )

        if self._retry_pending:
            self._next_retry_time = now + self.retry_interval

            logger.warning(
                f"DB_RETRY_SCHEDULED vitals_pending = {len(self._vitals_buffer)} "
                f"alerts_pending = {len(self._alerts_buffer)} retry_after_seconds = {self.retry_interval:.2f}"
            )
        else:
            self._next_retry_time = 0.0
        if self.metrics.enabled:
            self.metrics.gauge("buffer_depth", len(self._vitals_buffer), labels = {"stream":"vitals"})
            self.metrics.gauge("buffer_depth", len(self._alerts_buffer), labels = {"stream":"alerts"})
    
    def _parse_observed_at(self, value: str) -> datetime:
        """Đọc timestamp có múi giờ và chuẩn hóa sang UTC.

        Không tự gán múi giờ cho timestamp thiếu offset vì có thể diễn giải
        sai thời điểm đo. Producer phải gửi ISO 8601 có offset hoặc hậu tố Z.
        """
    
        observed_at = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if observed_at.utcoffset() is None:
            raise ValueError("recorded_at is missing a timezone; producer must send a UTC timestamp")
        return observed_at.astimezone(timezone.utc)

    def _submit_agent_event(self, msg, surgery_id:int, observed_at: datetime, risk_level:  str, rule_reasons, features, ml_score ):
        """Gửi snapshot nhất quán của lần đánh giá sang agent.

        Gửi khi có điểm ML mới hoặc cảnh báo CRITICAL. Chất lượng tạm đặt
        PARTIAL khi có điểm ML vì feature hiện chưa phản ánh đầy đủ tuổi
        tín hiệu và dữ liệu thiếu trước forward-fill.
        """
        if ml_score is None and risk_level != "CRITICAL":
            return
        
        event = RiskEvent(
            event_id  = (f"{msg.topic()}:{msg.partition()}:{msg.offset()}"),
            surgery_id = surgery_id,
            observed_at = observed_at,
            current_rule_level = risk_level,
            rule_reasons = tuple(rule_reasons),
            model_score = ml_score,
            horizon_seconds = self.ml_model.prediction_horizon_seconds,
            model_version =  self.ml_model.model_version,
            data_quality = "PARTIAL" if ml_score is not None else "UNKNOWN",
            features =  dict(features or {}),
            context = {},
        )

        self.agent_runtime.submit(event)

    def start_listening(self):
        logger.info(f"Consumer is listening to topic: {self.topic} ..." 
                    f"(batch_size = {self.batch_size}, flush_interval = {self.flush_interval})")
        try:
            self.agent_runtime.start()
            if self.signal_monitor is not None:
                self.signal_monitor.start()
            while True:
                # Poll data each second
                self.agent_runtime.check_health()
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

                    bench =  vitals_data.pop("_bench", None)
                    rid = None
                    if self.metrics.enabled:
                        # Chỉ lọc run trong chế độ benchmark.
                        if not isinstance(bench, dict) or bench.get("run_id") != self.metrics.run_id:
                            continue

                        rid = record_id(self.metrics.run_id, surgery_id, vitals_data["recorded_at"])
                        self.metrics.trace("consumer_received", rid, source_ns = bench["t0_ns"])
                        self.signal_monitor.update_signal(surgery_id, vitals_data, bench["t0_ns"])

                    self._vitals_buffer.append((surgery_id, vitals_data))

                    # Rules use each reading; ML uses the configured streaming window.
                    with  self.metrics.timer("rules_latency_ms"):
                        risk_level, rule_reasons = self.rule_engine.evaluate(vitals_data)
                    if self.metrics.enabled:
                        self.metrics.trace("rule_evaluated", rid, risk_level = risk_level)
                        if risk_level == "CRITICAL":
                            self.metrics.trace("critical_detected", rid)
                    observed_at = self._parse_observed_at(vitals_data["recorded_at"])
                    
                    ml_score = None
                    features =None
                    try:
                        with self.metrics.timer("feature_and_ml_latency_ms"):
                            features = self.feature_builder.process_reading(
                                surgery_id, vitals_data, timestamp=observed_at.timestamp()
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
                        if self.metrics.enabled:
                            self.metrics.trace(
                                "alert_generated", rid,
                                event_id=logical_alert_id(rid),
                                alert_kind="critical_rule" if risk_level == "CRITICAL" else "ml_only",
                            )
                    
                    self._submit_agent_event(
                        msg = msg, 
                        surgery_id = surgery_id,
                        observed_at = observed_at,
                        risk_level = risk_level,
                        rule_reasons = rule_reasons,
                        features = features,
                        ml_score = ml_score,
                    )
                    if self._should_flush():
                        self._flush()
                except AgentRuntimeError:
                    raise
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue    
        except KeyboardInterrupt:
            logger.info("Receive command to stop the consumer")
        finally:
            try:
                self.agent_runtime.close()
            finally:
                try:
                    self._flush()

                    if self._vitals_buffer or self._alerts_buffer:
                        raise RuntimeError(
                            "Shutdown did not persist all data: "
                            f"vitals={len(self._vitals_buffer)}, "
                            f"alerts={len(self._alerts_buffer)}. "
                            "Records remaining in RAM have not been persisted.")
                finally:   
                    try:
                        self.consumer.close()
                    finally:
                        try:
                            self.db_client.close()
                        finally:
                            try:
                                if self.signal_monitor is not None:
                                    self.signal_monitor.close()
                            finally:
                                self.metrics.close()


if __name__ == "__main__":
    # Independent consumer test run
    config = AppConfig.load_config()
    consumer = VitalsConsumer(config)
    consumer.start_listening()
