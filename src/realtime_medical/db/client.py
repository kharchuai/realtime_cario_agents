# =========================================================================
# Module: db/client.py (DataBaseClient)
#
# Tác dụng: Lớp trung gian DUY NHẤT giữa ứng dụng và Postgres - mọi thao
# tác đọc/ghi DB (lưu vitals, lưu cảnh báo) đều đi qua class này, không
# module nào khác tự viết SQL riêng.
#
# THIẾT KẾ ĐỒNG BỘ VỚI config.yaml:
#   Câu lệnh INSERT cho bảng vitals được build ĐỘNG dựa trên
#   config.database.table và config.database.columns (section 4 trong
#   config.yaml), KHÔNG hardcode tên bảng/tên cột trong code.
#
#   Điều này chạy được vì tên cột trong config.database.columns
#   ("hr", "spo2", "sbp"...) TRÙNG với canonical key trong dict vitals
#   (canonical key này đến từ config.source_mapping.fields khi build
#   ở sensor_simulator.py / consumer.py) - cả hệ thống dùng chung 1 bộ
#   tên field duy nhất xuyên suốt từ nguồn dữ liệu -> DB.
#
#   Kết quả: nếu sau này bạn thêm/bớt cột trong config.database.columns
#   (ví dụ thêm đo SpO2 lần 2, thêm chỉ số mới), CHỈ CẦN SỬA config.yaml -
#   không cần sửa insert_vitals() trong file này, miễn là tên cột mới
#   cũng khớp với 1 canonical key nào đó trong vitals dict.
#
#   Bảng triage_alerts (cảnh báo) hiện CHƯA có section riêng trong
#   config.yaml nên vẫn khai báo cột tĩnh trong insert_alert() - nếu
#   muốn đồng bộ hoá luôn phần này, cần thêm 1 section "alerts_table"
#   vào config.yaml tương tự "database".
# =========================================================================
import logging
from typing import Any, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from src.realtime_medical.config import AppConfig

logger = logging.getLogger(__name__)

# Tên cột không đến từ dict "vitals" (đo lường thực tế), mà là dữ liệu
# ngữ cảnh của request (bệnh nhân nào, thời điểm nào). Khai báo tách riêng
# để _build_insert_values() biết lấy giá trị từ đâu thay vì tìm trong vitals.
META_COLUMN_SOURCES = {
    "surgery_id": lambda surgery_id, vitals: surgery_id,
    "recorded_at":  lambda surgery_id, vitals: vitals.get("recorded_at"),
    "time_sec": lambda surgery_id, vitals: vitals.get("time_sec"),
}

class DataBaseClient:
    def __init__(self, config:AppConfig):
        # Initialize the postgressSQL connection engine base on the DSN string in the configuration file
        self.engine = create_engine(config.postgres_dsn)
        self.Session = sessionmaker(bind= self.engine)
        
        self.vitals_table = config.database.table
        self.vitals_columns = config.database.columns
        
        logger.info("Database connection successfully initialized")
    
    def close(self):
        self.engine.dispose()
        logger.info("Database engine disposed")
    # ---------------------------------------------------------------
    # Helper dùng chung: gộp logic session/try/except/rollback lặp lại
    # giữa insert_vitals và insert_alert, để sau này thêm hàm insert
    # mới không phải copy-paste lại toàn bộ boilerplate.
    # ---------------------------------------------------------------
    def _execute_write(self, query, params:dict, context:str) -> Optional[Any]:
        with self.Session() as session:
            try:
                result = session.execute(query, params)
                session.commit()
                return  result
            except IntegrityError as e:
                session.rollback()
                logger.error(f"Data constraint violation during write ({context}). "
                            f"Check if surgery_id already exist in the 'surgeries' table: {e}")
                raise
            
            except Exception as e:
                session.rollback()
                logger.error(f"Error saving data to the database for patient {context}: {e}")
                raise
            
    def _build_insert_values(self, surgery_id: int, vitals: dict ) -> dict:
        """Ghép giá trị cho từng cột trong config.database.columns, lấy từ
        META_COLUMN_SOURCES (context) hoặc trực tiếp từ vitals (đo lường)."""
        values ={}
        for col  in self.vitals_columns:
            if col in META_COLUMN_SOURCES:
                values[col] = META_COLUMN_SOURCES[col](surgery_id, vitals)
            else:
                values[col] = vitals.get(col)
        return values
                
    def insert_vitals(self, surgery_id: int, vitals: dict) -> Tuple[int, Any]:
        """
        Lưu 1 bản ghi sinh hiệu vào bảng vitals (tên bảng + danh sách cột
        lấy từ config.database - xem docstring đầu file).
        Trả về record_id vừa tạo, dùng làm khoá ngoại khi lưu alert.
        """
        values =  self._build_insert_values(surgery_id, vitals)
        
        if values.get("recorded_at") is None:
            raise ValueError(
                "Vitals is missing ''recorded_at' - this is part of the Primary Key"
                "for the vitals_signs table an is mandatory "
            )
        col_list = ",".join(self.vitals_columns)
        placeholder_list = ",".join(f":{c}" for c in self.vitals_columns)
        
        query = text(
            f"INSERT INTO {self.vitals_table} ({col_list}) "
            f"VALUES ({placeholder_list}) RETURNING surgery_id, recorded_at;"
        )
        
        result = self._execute_write(query, values, context = f"insert_vitals surgery_id = {surgery_id}")
        row = result.first()
        return row.surgery_id, row.recorded_at
            
            
    def insert_vitals_batch(self, rows:list) -> int:
        if not rows:
            return 0
        all_values = {}
        value_groups = []
        
        for i, (surgery_id, vitals) in enumerate(rows):
            row_values = self._build_insert_values(surgery_id, vitals)
            if row_values.get("recorded_at") is None:
                raise ValueError(
                    f"Row {i} (surgery_id={surgery_id}) lack 'recorded_at'"
                )
            placeholders = []
            for col in self.vitals_columns:
                key = f"{col}_{i}"
                all_values[key] = row_values[col]
                placeholders.append(f":{key}")
            value_groups.append("("+", ".join(placeholders) + ")")
                
        col_list = ", ".join(self.vitals_columns)
        values_clause = ", ".join(value_groups)
        query = text(
            f"INSERT INTO {self.vitals_table} ({col_list}) "
            f"VALUES {values_clause} "
            f"ON CONFLICT (surgery_id, recorded_at) DO NOTHING "
            f"RETURNING surgery_id;"
        )
        
        result = self._execute_write(query, all_values, context = f"insert_vitals_batch n ={len(rows)}")
        return len(result.fetchall())   
                
            
            
            
    def insert_alert(
            self, 
            surgery_id:int, 
            recorded_at:Any, 
            risk_level: str, 
            model_score:float, 
            alert_reason: str
            ) -> None:
        """
        Lưu cảnh báo nguy hiểm vào bảng triage_alerts khi RuleEngine hoặc
        MLTriageModel phát hiện rủi ro.

        (surgery_id, recorded_at) phải khớp đúng 1 bản ghi đã tồn tại
        trong vital_signs - đây là FK composite theo schema_1.sql, KHÔNG
        có cột patient_id hay vitals_record_id trong bảng triage_alerts.
        """
        """ 
        Save risk warning information to the database when an indicator exceeds threshold or an ML model predicts a danger
        """
        
        query = text("""
                    INSERT INTO triage_alerts (
                        surgery_id, recorded_at, risk_level, model_score, alert_reason
                    )
                    VALUES(
                        :surgery_id, :recorded_at, :risk_level, :model_score, :alert_reason
                    )
                    """)
            
        self._execute_write(
            query,
            {
                "surgery_id": surgery_id,
                "recorded_at": recorded_at,
                "risk_level": risk_level,
                "model_score": model_score,
                "alert_reason": alert_reason,
            },
            context=f"insert_alert surgery_id = {surgery_id}",
        )
        
        logger.info(f"Alert saved to the database for surgery_id  = {surgery_id}")
        
        
    def insert_alerts_batch(self, alerts: list) -> int:
        if not alerts:
            return 0
        
        columns =  ["surgery_id", "recorded_at", "risk_level", "model_score", "alert_reason"]
        all_values = {}
        value_groups = []
        
        for i, alert in enumerate(alerts):
            placeholders = []
            for col in columns:
                key = f"{col}_{i}"
                all_values[key] = alert[col]
                placeholders.append(f":{key}")
            value_groups.append("("+", ".join(placeholders) +  ")")
                
        col_list = ", ".join(columns)
        values_clause = ", ".join(value_groups)
        query = text(f"INSERT INTO triage_alerts ({col_list}) VALUES {values_clause}")
        result = self._execute_write(query, all_values, context = f"insert_alerts_batch n = {len(alerts)}")
        logger.info(f"Batch saved {len(alerts)} alert(s) to the database")
        
        return result.rowcount 