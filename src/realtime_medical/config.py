"""
Module cấu hình trung tâm (config.py)
 
- Mỗi class con bên dưới tương ứng với 1 section trong configs/config.yaml
  (đánh số 1 -> 6 khớp với comment trong file yaml).
- AppConfig gộp tất cả section lại, đồng thời bổ sung thêm giá trị đọc từ
  biến môi trường (kafka_broker_url, postgres_*) - dùng cho thông tin
  nhạy cảm/theo môi trường, không commit lên git.
- Pydantic tự động validate toàn bộ dữ liệu khi load (đúng kiểu, đủ field
  bắt buộc) -> sai cấu hình sẽ báo lỗi ngay lúc khởi động.
- load_config() đọc config.yaml, parse thành AppConfig đã validate.
- `settings` là object global duy nhất, các module khác import dùng chung:
      from src.realtime_medical.config import settings
"""
# =============================================================
# Module cấu hình trung tâm (config.py)
#
# - Mỗi class con (StreamingConfig, TriageRulesConfig, ModelConfig)
#   tương ứng với 1 section trong file configs/config.yaml.
#
# - AppConfig gộp tất cả các class con lại (tổng hợp), đồng thời bổ
#   sung thêm các giá trị đọc từ biến môi trường (kafka_broker_url,
#   postgres_dsn...). Biến môi trường dùng cho thông tin nhạy cảm/
#   theo môi trường (mật khẩu DB, host, port...) vì không nên commit
#   lên git, khác với config.yaml chỉ chứa cấu hình nghiệp vụ.
#
# - Pydantic tự động validate toàn bộ dữ liệu khi load (đúng kiểu,
#   đủ field bắt buộc) -> sai cấu hình sẽ báo lỗi ngay lúc khởi động,
#   không đợi đến lúc chạy giữa chừng mới lỗi.
#
# - Hàm load_config() đọc file config.yaml, parse thành object
#   AppConfig đã validate. Sau này chỉ cần sửa config.yaml (hoặc
#   biến môi trường), không cần sửa code ở các module khác.
#
# - Object `settings` được khởi tạo 1 lần duy nhất tại đây và được
#   các file khác import dùng chung:
#       from src.realtime_medical.config import settings
#
# Tác dụng chính: làm lớp trung gian duy nhất giữa (config.yaml +
# biến môi trường) và toàn bộ logic xử lý của ứng dụng.
# =============================================================
from pathlib import Path
from  typing  import Dict, List
from urllib.parse import quote_plus

import yaml 
import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR =  Path(__file__).resolve().parent.parent.parent

# Metrics/logging also read os.environ directly, before AppConfig is instantiated.
load_dotenv(BASE_DIR / ".env", override=False)

# 1. canonical_fields
class CanonicalFieldsConfig(BaseModel):
    heart_rate: str
    spo2: str
    systolic_bp: str
    diastolic_bp: str
    mean_bp: str
    respiratory_rate: str
    body_temp: str
    
# 2 source_mapping:
class SourceMappingConfig(BaseModel):
    vendor: str
    fields:  Dict[str, str]
    
# 3. streaming
class MissingDataConfig(BaseModel):
    max_gap_seconds: int
    interpolation: str
    signal_loss_alert: bool
    
class ArtifactRange(BaseModel):
    valid_min: float
    valid_max: float
    
class StreamingConfig(BaseModel):
    window_size_seconds:  int
    evaluate_every_seconds:  int
    min_samples_to_evaluate: int 
    batch_size: int
    flush_interval_seconds:int
    sampling_rate_seconds:  int
    aggregate_by: str
    missing_data:  MissingDataConfig
    artifact_filtering: Dict[str, ArtifactRange]
    
# Database
class DatabaseConfig(BaseModel):
    table: str
    columns:  List[str]
    
# triage_rules
class HeartRateRule(BaseModel):
    high: int
    low: int
    
class Spo2Rule(BaseModel):
    critical: int
    
class BloodPressureRule(BaseModel):
    sbp_high: int
    sbp_low: int
    dbp_high:  int
    dbp_low: int
    mbp_low: int
    
class RespiratoryRateRule(BaseModel):
    high:  int
    low: int 
    
class BodyTemperatureRule(BaseModel):
    high: float
    low: float

class CombinedRule(BaseModel):
    name: str
    description: str
    condition: str
    
class TriageRulesConfig(BaseModel):
    heart_rate: HeartRateRule
    spo2: Spo2Rule  
    blood_pressure: BloodPressureRule
    respiratory_rate: RespiratoryRateRule
    body_temperature: BodyTemperatureRule
    combined_rules: List[CombinedRule] = Field(default_factory= list)
    
# 6. model
class ModelConfig(BaseModel):
    path: str
    evaluation_metric: str
    prediction_horizon_seconds: int 
    
    
# AppConfig
class AppConfig(BaseSettings):
    model_config  =  SettingsConfigDict(
        env_file = ".env",
        env_file_encoding="utf-8",
        extra =  "ignore",
        )
    # from .yaml file 
    project_name: str
    canonical_fields: CanonicalFieldsConfig
    source_mapping:  SourceMappingConfig
    streaming:  StreamingConfig
    database:  DatabaseConfig
    triage_rules: TriageRulesConfig
    model: ModelConfig
    
    # from .env file
    kafka_broker_url:  str = "localhost:9092"
    kafka_topic_vitals: str = "patient_vitals_stream"

    postgres_user: str = "postgres"
    postgres_password:  str = ""
    postgres_host: str = "localhost"
    # Docker publishes PostgreSQL's internal 5432 port on host port 5433.
    postgres_port: str = "5433"
    postgres_db: str = "medical_realtime"

    @property
    def postgres_dsn(self) -> str:
        """ Construct a PostgreSQL connection string, autommatically escaping specical characters in the password """
        return (
            f"postgresql://{self.postgres_user}:{quote_plus(self.postgres_password)}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
        
    @classmethod
    def load_config(cls, config_path: str| Path= BASE_DIR / "configs"/ "config.yaml") -> "AppConfig":
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found at:  {config_path}")
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data =yaml.safe_load(f)
            
        return cls(**yaml_data)

# Initialize a global instance for import in other files
# from src.realtime_medical.config    
settings = AppConfig.load_config()

    




