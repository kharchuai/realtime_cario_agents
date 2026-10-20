# Hệ thống theo dõi sinh hiệu thời gian thực

Pipeline phát lại dữ liệu sinh hiệu từ CSV qua Kafka. Consumer xử lý bằng rules và model XGBoost, lưu sinh hiệu/cảnh báo vào PostgreSQL; agent tổng hợp kết quả và giải thích trong log.

```text
CSV → Producer → Kafka → Consumer → PostgreSQL
                            ├── Rules
                            ├── Sliding window → ML
                            └── Agent → Log
```

## Yêu cầu

- Python 3.11 và pip cho các lệnh chạy trên máy host.
- Docker Desktop đang chạy, có Docker Compose.
- Model tại `models/heart_disease_classifier.pkl`.
- Dữ liệu tại `data/clinical_data.csv` và `data/streaming/case_<id>_numerics.csv`.

Các ví dụ dùng **PowerShell trên Windows**, chạy tại thư mục gốc repository. Gọi trực tiếp `python` trong môi trường đang sử dụng.
## Cấu trúc dự án

```text
realtime_cario_agents/
├── configs/
│   └── config.yaml                 # Cấu hình pipeline, rules và model
├── data/
│   ├── clinical_data.csv           # Thông tin lâm sàng
│   ├── clinical_parameters.csv     # Tham số lâm sàng
│   └── streaming/                  # CSV sinh hiệu cho từng case
├── db/
│   ├── schema_1.sql                # Cấu trúc database
│   └── seed_patients.sql           # Dữ liệu khởi tạo
├── models/
│   └── heart_disease_classifier.pkl # Model ML đang sử dụng
├── scripts/
│   ├── run_pipeline.py             # Chạy replay baseline hoặc fault
│   ├── run_fault.py                # Điều khiển fault
│   ├── seed_data_clinical.py       # Nạp dữ liệu bệnh nhân
│   └── *benchmark.py               # Phân tích và tổng hợp metrics
├── src/realtime_medical/
│   ├── streaming/                  # Producer, consumer, sliding window
│   ├── triage/                     # Rules và suy luận ML
│   ├── agent/                      # Xử lý và giải thích cảnh báo
│   ├── db/                         # Kết nối PostgreSQL
│   ├── utils/                      # Metrics, report và fault injection
│   ├── generator/                  # Mô phỏng dữ liệu cảm biến
│   ├── feedback/                   # Logic retrain
│   ├── config.py                   # Đọc cấu hình
│   └── main.py                     # Điểm vào của consumer
├── test/                           # Kiểm thử pipeline và metrics
├── docs/                           # Tài liệu bổ sung
├── logs/                           # Log và report của từng lượt chạy
├── tmp/benchmark/                  # Trace metrics trung gian
├── .env.example                    # Mẫu biến môi trường
├── docker-compose.yml              # PostgreSQL, Kafka và consumer
├── Dockerfile                      # Image của consumer
├── requirements.txt                # Thư viện Python
└── README.md
```

Luồng chính: `run_pipeline.py` đọc CSV và phát lên Kafka; consumer áp dụng rules, ML và agent rồi ghi kết quả vào PostgreSQL. Khi bật metrics, report cuối nằm trong `logs/<run-id>/report.json`.

## Cấu trúc dự án

```text
realtime_cario_agents/
├── configs/
│   └── config.yaml                   # Cấu hình pipeline, rules và model
├── data/
│   ├── clinical_data.csv             # Thông tin lâm sàng
│   ├── clinical_parameters.csv       # Tham số lâm sàng
│   └── streaming/                    # CSV sinh hiệu cho từng case
├── db/
│   ├── schema_1.sql                  # Cấu trúc database
│   └── seed_patients.sql             # Dữ liệu khởi tạo
├── models/
│   └── heart_disease_classifier.pkl  # Model ML đang sử dụng
├── scripts/
│   ├── run_pipeline.py               # Chạy replay baseline hoặc fault
│   ├── run_fault.py                  # Điều khiển fault
│   ├── seed_data_clinical.py         # Nạp dữ liệu bệnh nhân
│   └── *benchmark.py                 # Phân tích và tổng hợp metrics
├── src/realtime_medical/
│   ├── streaming/                    # Producer, consumer, sliding window
│   ├── triage/                       # Rules và suy luận ML
│   ├── agent/                        # Xử lý và giải thích cảnh báo
│   ├── db/                           # Kết nối PostgreSQL
│   ├── utils/                        # Metrics, report và fault injection
│   ├── generator/                    # Mô phỏng dữ liệu cảm biến
│   ├── config.py                     # Đọc cấu hình
│   └── main.py                       # Điểm vào của consumer
├── test/                             # Kiểm thử pipeline và metrics
├── docs/                             # Tài liệu bổ sung
├── logs/                             # Log và report của từng lượt chạy
├── tmp/benchmark/                    # Trace metrics trung gian
├── .env.example                      # Mẫu biến môi trường
├── docker-compose.yml                # PostgreSQL, Kafka và consumer
├── Dockerfile                        # Image của consumer
├── requirements.txt                  # Thư viện Python
└── README.md
```

Luồng chính: `run_pipeline.py` đọc CSV và phát lên Kafka; consumer áp dụng rules, ML và agent rồi ghi kết quả vào PostgreSQL. Khi bật metrics, report cuối nằm trong `logs/<run-id>/report.json`.

## Cài đặt

Cài dependencies bằng Python của môi trường hiện tại:

```powershell
python --version
python -m pip install -r requirements.txt
```

Tạo `.env` từ file mẫu nếu chưa có:

```powershell
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
```

Điền thông tin PostgreSQL trong `.env` cho khớp database đang dùng. Kết nối trên máy host sử dụng:

```dotenv
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
KAFKA_BROKER_URL=localhost:9092
ML_INFERENCE_N_JOBS=1
```

## Khởi động dịch vụ

Mở Docker Desktop rồi chạy:

```powershell
docker compose up -d postgres zookeeper kafka kafka-init
docker compose build consumer
docker compose ps -a
```

Chờ PostgreSQL/Kafka ở trạng thái healthy và `kafka-init` kết thúc với mã 0. Nạp thông tin bệnh nhân, ca mổ nếu DB chưa có dữ liệu cần chạy:

```powershell
python -B -m scripts.seed_data_clinical --patients 50
```

Case ID dùng khi replay phải tồn tại trong DB và có CSV tương ứng. `--patients` là số bệnh nhân được seed, còn `--cases` là danh sách ID ca mổ.

Chỉ build lại consumer khi thay code, dependencies, model hoặc config. Các lượt tiếp theo dùng lại image đã build.

## Chạy pipeline

### Không metrics

```powershell
$env:BENCH_ENABLED = '0'
docker compose up -d --no-deps --force-recreate consumer
docker compose logs --tail 30 consumer
```

Chờ log có `KAFKA_ASSIGNED`, sau đó phát dữ liệu:

```powershell
python -B scripts/run_pipeline.py --cases 1 2 3 --workers 3 --data-dir data/streaming --max-rows 200 --realtime --verify-timeout 120
```

### Baseline có metrics

```powershell
$env:BENCH_ENABLED = '1'
python -B scripts/run_pipeline.py --cases 1 2 3 --workers 3 --data-dir data/streaming --max-rows 1200 --realtime --verify-timeout 120
```

Runner tự tạo run ID, khởi động lại consumer, chờ sẵn sàng, phát dữ liệu, kiểm tra DB, dừng consumer và xuất report. Không cần mở consumer riêng hay chạy analyzer thủ công. Kafka và PostgreSQL vẫn chạy.

### Fault có metrics

Dùng cùng nguồn CSV, pause consumer 20 giây tại giây nguồn 300 rồi tự unpause:

```powershell
$env:BENCH_ENABLED = '1'
python -B scripts/run_pipeline.py --cases 1 2 3 --workers 3 --data-dir data/streaming --max-rows 1200 --realtime --verify-timeout 120 --fault consumer_pause --fault-at 300 --fault-duration 20
```

Nguồn tiếp tục phát trong lúc pause; cần có dữ liệu qua giây 320 và đủ mẫu sau khi consumer phục hồi. Chạy baseline và fault tuần tự.

Nếu cần probe đều để đo M8, thêm `--critical-probes` vào cả hai lệnh baseline/fault. Cờ này thay giá trị sinh hiệu bằng probe critical tổng hợp theo nhịp 1 giây, không dùng để đánh giá dự đoán ML trên CSV gốc.

### Chạy nhiều case đồng thời

Ví dụ 50 case ở chế độ metrics:

```powershell
$env:BENCH_ENABLED = '1'
$cases = 1..50
python -B scripts/run_pipeline.py --cases $cases --workers 50 --data-dir data/streaming --max-rows 1200 --realtime --verify-timeout 120
```

Muốn đúng 50 bệnh nhân khác nhau, kiểm tra các case tương ứng 50 patient ID khác nhau.

| Tham số | Ý nghĩa |
|---|---|
| `--cases` | Danh sách ID ca mổ cần phát |
| `--workers` | Số case phát đồng thời; đặt bằng số case để không phải chờ, mặc định tối đa 10 |
| `--max-rows` | Số hàng CSV đọc tối đa mỗi case; `0` để đọc toàn bộ |
| `--realtime` | Giữ nhịp theo `time_sec` của CSV; probe dùng nhịp 1 giây/mẫu hợp lệ |
| `--verify-timeout` | Thời gian chờ kiểm tra DB sau khi phát xong, tính bằng giây |
| `--fault-at` | Thời điểm tạo fault tính từ mốc bắt đầu nguồn, đơn vị giây |
| `--fault-duration` | Thời gian pause consumer, đơn vị giây |

`--max-rows 1200` không bảo đảm 1200 mẫu hợp lệ hoặc đủ 20 phút: thời lượng tùy timeline CSV và chế độ replay/probe; hàng trống hoặc timestamp trùng có thể bị bỏ.

## Log và report

```text
logs/pipeline/runner.log          Log runner, ghi nối tiếp
logs/pipeline/consumer.log        Log consumer, ghi nối tiếp
logs/<run-id>/report.json         Report khi bật metrics
tmp/benchmark/<run-id>/           Trace metrics và db_keys.json
```

Sinh hiệu và alert được lưu trong PostgreSQL. Khi bật metrics, runner in đường dẫn ở dòng `REPORT`; không cần tự khai báo `BENCH_RUN_ID`, `METRICS_DIR` hoặc `BENCH_CASES`.

Theo dõi log consumer khi đang chạy:

```powershell
docker compose logs -f consumer
```

## Dừng dịch vụ

Sau khi lượt chạy hoàn tất:

```powershell
docker compose stop
```
