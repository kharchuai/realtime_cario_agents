# Hệ thống theo dõi sinh hiệu và cảnh báo nguy cơ

## 1. Giới thiệu

Dự án xây dựng một pipeline xử lý dữ liệu sinh hiệu theo thời gian thực. Dữ liệu được phát lại từ CSV, gửi qua Kafka, sau đó consumer xử lý bằng rules và mô hình học máy. Sinh hiệu và cảnh báo được lưu vào PostgreSQL. Agent chạy nền để tổng hợp kết quả, bằng chứng và phần giải thích.

Các sinh hiệu hiện có gồm nhịp tim, SpO2, huyết áp tâm thu, huyết áp tâm trương, huyết áp trung bình, nhịp thở và nhiệt độ cơ thể.

Dự án phục vụ việc thử nghiệm pipeline, xử lý nhiều bệnh nhân đồng thời và đánh giá độ trễ, dữ liệu thiếu, khả năng phục hồi khi có lỗi. Kết quả agent hiện được xuất ra console; cảnh báo được lưu trong database.

## 2. Công nghệ sử dụng

- Python 3.11.
- Apache Kafka và ZooKeeper.
- PostgreSQL 15, SQLAlchemy.
- Pandas, NumPy, scikit-learn và XGBoost.
- Pydantic để đọc và kiểm tra cấu hình.
- Docker và Docker Compose.
- unittest để kiểm thử.

Model hiện tại là XGBClassifier với 61 features, được lưu trong một bundle gồm pipeline, danh sách features và metadata. Nguồn nhịp thở hiện dùng `Solar8000/RR_CO2`, map về `rr` trong cấu hình. Agent được tổ chức thành các bước xử lý bằng Python.

## 3. Luồng xử lý

```text
CSV sinh hiệu
    |
    v
Producer -> Kafka -> Consumer
                        |
                        +-> Rules trên từng reading
                        |
                        +-> Sliding window -> Features -> Model
                        |
                        +-> Lưu vitals và alerts vào PostgreSQL
                        |
                        +-> Queue agent -> Tổng hợp và giải thích -> Console
```

Rules kiểm tra dữ liệu hiện tại theo ngưỡng trong cấu hình. Model sử dụng features của cửa sổ trượt và thông tin tĩnh của ca mổ để dự đoán nguy cơ. Agent nhận snapshot đã được consumer tạo và xử lý trong một worker nền.

## 4. Cấu trúc thư mục

```text
configs/
    config.yaml                 Cấu hình sinh hiệu, streaming, rules và model
data/
    clinical_data.csv           Thông tin bệnh nhân và ca mổ
    streaming/                  CSV sinh hiệu theo case
db/
    schema_1.sql                Schema database
docs/                           Hướng dẫn và báo cáo
models/                         Model bundle và metadata
scripts/
    seed_data_clinical.py        Nạp bệnh nhân và ca mổ vào DB
    train_model_fix.py          Huấn luyện và chọn model trên validation
    run_pipeline.py             Phát nhiều case và kiểm tra kết quả DB
    run_benchmark_suite.py      Chạy các kịch bản benchmark
    run_fault.py                Tạo pause fault thủ công
    analyze_benchmark.py        Tính metrics cho một run
    summarize_benchmark.py      Gộp số liệu thành summary.md và summary.json
    review_benchmark.py         Kiểm tra trace, hash và DB
src/realtime_medical/
    agent/                      Các bước xử lý và worker agent
    db/                         Thao tác ghi database
    generator/                  Đọc CSV và giả lập nguồn sinh hiệu
    streaming/                  Producer, consumer và feature window
    triage/                     Rules và model inference
    utils/                      Metrics, signal monitor và fault injection
    config.py                   Schema cấu hình
    main.py                     Khởi động consumer
test/
    test_*.py                   Các bài kiểm thử
    benchmark_results/          Trace, log, fixtures và kết quả benchmark
docker-compose.yml              Các service của hệ thống
Dockerfile                      Image chạy ứng dụng
requirements.txt                Thư viện Python
```

Thư mục `test/benchmark_results/` được Git ignore và không đưa vào Docker build context. Nó được tạo khi chạy benchmark nên có thể chưa xuất hiện trên máy mới clone dự án.

## 5. Cài đặt

### 5.1. Chuẩn bị

Cần có Docker Desktop và Docker Compose. Nếu chạy các script từ máy host, cần có Python và thư viện của dự án. Các lệnh bên dưới dùng PowerShell và chạy tại thư mục gốc repository.

Để tạo môi trường mới, dùng Python 3.11 cho các phiên bản được pin trong requirements. Các lệnh sau giả định `python` đang trỏ đến Python 3.11:

```powershell
python --version
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Nếu đã có môi trường Python chạy được các script thì có thể sử dụng môi trường đó.

### 5.2. Cấu hình môi trường

Tạo `.env` từ file mẫu nếu chưa có:

```powershell
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
```

Cập nhật user, password và tên database trong `.env` cho đúng môi trường. File `.env` không được đưa lên Git. Nếu DB đã tồn tại trong Docker volume, sử dụng thông tin đăng nhập đã dùng khi khởi tạo DB.

Kết nối từ host dùng PostgreSQL ở `localhost:5433` và Kafka ở `localhost:9092`. Các container dùng PostgreSQL `postgres:5432` và Kafka `kafka:29092`; Compose đã cấu hình các địa chỉ nội bộ này.

### 5.3. Khởi động Docker

```powershell
$env:BENCH_ENABLED = '0'
$env:BENCH_ALERT_ACK_LOSS = '0'
$env:ML_INFERENCE_N_JOBS = '1'

docker compose up -d --build
docker compose ps -a
docker compose logs --tail 30 consumer
```

Consumer bắt đầu nhận dữ liệu sau khi Kafka assign partition. Có thể theo dõi trực tiếp bằng:

```powershell
docker compose logs -f consumer
```

Schema SQL trong `db/` chạy khi khởi tạo PostgreSQL lần đầu. Thay schema file không tự cập nhật một DB đã có dữ liệu; thay đổi schema của DB đang dùng cần migration riêng.

## 6. Chạy pipeline không đo metrics

Nạp thông tin bệnh nhân và ca mổ nếu DB chưa có dữ liệu. Chạy lệnh dưới đây từ thư mục gốc, trong môi trường Python đã có dependencies:

```powershell
python -B -m scripts.seed_data_clinical --patients 40
```

Script đọc `data/clinical_data.csv`, chọn số bệnh nhân được yêu cầu và nạp các ca mổ tương ứng. Case IDs dùng để replay phải tồn tại trong DB và có CSV tương ứng.

Thiết lập chế độ không đo, rồi recreate consumer:

```powershell
$env:BENCH_ENABLED = '0'
$env:BENCH_ALERT_ACK_LOSS = '0'
$env:BENCH_CRITICAL_PROBES = '0'
$env:BENCH_DROP_FIELD = ''
$env:ML_INFERENCE_N_JOBS = '1'
$env:POSTGRES_HOST = 'localhost'
$env:POSTGRES_PORT = '5433'
$env:KAFKA_BROKER_URL = 'localhost:9092'

Remove-Item Env:BENCH_RUN_ID -ErrorAction SilentlyContinue
Remove-Item Env:METRICS_DIR -ErrorAction SilentlyContinue
Remove-Item Env:KAFKA_TOPIC_VITALS -ErrorAction SilentlyContinue

docker compose up -d --no-deps --force-recreate consumer
docker compose logs --tail 30 consumer
```

Sau khi thấy `KAFKA_ASSIGNED`, phát dữ liệu cho các case cần kiểm tra:

```powershell
python -B scripts/run_pipeline.py --cases 1 2 3 --workers 3 --data-dir data/streaming --max-rows 200 --realtime --verify-timeout 120
```

`--realtime` giữ khoảng cách thời gian theo `time_sec` của CSV. CSV thật có thể có hàng trống hoặc các mốc cách xa nhau, nên số readings hợp lệ và thời gian chạy tùy dữ liệu. Có thể chọn case khác hoặc tăng `--max-rows` nếu phần đầu file chưa có sinh hiệu.

Ở chế độ này không cần run ID, thư mục metrics hoặc `_bench` trong payload. Rules, model, agent và ghi DB vẫn hoạt động.

## 7. Chạy benchmark

Suite benchmark hiện hỗ trợ 30–50 bệnh nhân. Với `--patients 40`, script sử dụng surgery IDs 1–40 của 40 bệnh nhân khác nhau; cần bảo đảm những bản ghi này đã có trong DB.

```powershell
$env:POSTGRES_HOST = 'localhost'
$env:POSTGRES_PORT = '5433'
$env:KAFKA_BROKER_URL = 'localhost:9092'
$env:PYTHONIOENCODING = 'utf-8'

$suiteId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_40patients'
python -B scripts/run_benchmark_suite.py --suite-id $suiteId --patients 40
```

Script build image, tạo fixture và topic riêng, chờ consumer sẵn sàng rồi chạy các kịch bản. Chỉ chạy một suite tại một thời điểm vì các lượt cùng điều khiển container consumer.

Để replay dữ liệu thật có RR_CO2 và giảm fault xuống 10 giây, thêm `--source-mode replay --data-dir data/streaming --ticks 120 --window-anchor rr --pause-seconds 10 --gap-seconds 10 --drop-field rr`. Báo cáo ngày 16/10 theo form baseline nằm ở [review_baseline_rrco2_2026-10-16.md](docs/review_baseline_rrco2_2026-10-16.md); phần lệnh và giới hạn đo được ghi trong báo cáo. Các kịch bản mặc định dưới đây dùng probe tổng hợp, fault 20 giây.

| Kịch bản | Nội dung |
|---|---|
| baseline | Không tạo fault, phát dữ liệu critical tổng hợp |
| consumer_pause | Pause consumer 20 giây ở khoảng giây 35, nguồn vẫn phát |
| ack_loss | Commit batch alert đầu tiên rồi giả mất ACK để kiểm tra retry |
| signal_gap | Bỏ HR tại ticks 30–49, các trường khác vẫn được gửi |

Baseline nghĩa là hệ thống không bị gây lỗi. Fixture vẫn có SpO2 thấp và kỳ vọng có alert; không phải fixture sinh hiệu bình thường.

Chạy riêng một kịch bản:

```powershell
$runSuite = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_pause30'
python -B scripts/run_benchmark_suite.py --suite-id $runSuite --patients 30 --ticks 160 --scenarios consumer_pause --skip-build
```

Chỉ dùng `--skip-build` khi image đã chứa source và dependencies hiện tại. Khi đổi source, model hoặc config được COPY vào image, cần build lại.

Kết quả lưu tại:

```text
test/benchmark_results/<suite-id>/
    fixtures/
    suite.json
    normal_mode.json

test/benchmark_results/<suite-id>_<scenario>/
    report.json
    scenario.json
    db_keys.json
    *.jsonl
    *.health.json
    runner.log
    consumer.log
```

Đường dẫn tương ứng trong consumer là `/app/test/benchmark_results`. Compose bind mount thư mục kết quả của host vào đường dẫn này.

Mỗi lượt phải có ID mới để tránh ghi đè trace. Sau khi chạy xong, suite kiểm tra pipeline không đo metrics và chạy lại consumer thường.

### Các metrics đang dùng

| Metric | Nội dung |
|---|---|
| M1 | Missing-at-Deadline / Data Completeness |
| M2 | Signal Gap / Signal Age |
| M3 | On-time Service Availability / Deadline Miss |
| M4 | Critical Detection End-to-End |
| M5 | Alert Persistence Latency |
| M6 | Duplicate Logical Alerts |
| M7 | Critical-Path Continuity Under Fault |
| M8 | Observed Recovery Time / RTO |
| M9 | Backlog Drain Time |
| M10 | Sustained Throughput |

M4 hiện đo từ mốc nguồn đến `critical_detected` của rules và có thêm thống kê từ nguồn đến alert đã lưu. Nó chưa đo đến lúc agent hoàn tất. M7/M8/M9 nằm trong `M7_M8_M9_by_fault` đối với lượt có pause fault.

### Tổng hợp số liệu

Sau khi suite đã chạy xong, gộp số liệu và kiểm tra khi cần:

```powershell
python -B scripts/summarize_benchmark.py --suite-id $suiteId
python -B scripts/review_benchmark.py --suite-id $suiteId --check-db
```

`summary.md` là bảng số liệu để tự viết báo cáo; `summary.json` giữ số chi tiết. `checks.json` chứa kiểm tra trace, hash và DB, không đưa ra kết luận đạt/chưa đạt. Các file mặc định nằm trong `test/benchmark_results/<suite-id>/`. Cả hai script hỗ trợ `--supplement-id` để gộp thêm một suite, và `--output` để chọn đường dẫn. Review chỉ kết nối DB khi có `--check-db`; không dùng form Word.

## 8. Kiểm thử

Chạy các nhóm kiểm thử metrics, rules, windowing và agent:

```powershell
python -B -m unittest test.test_benchmark_regressions test.test_rules test.test_windowing test.test_agent -v
```

Các nhóm này không cần Kafka/PostgreSQL thật. Benchmark Docker là phép kiểm tra tích hợp riêng. Kết quả đọc từ `report.json` cần đi kèm kiểm tra trace audit và DB, không chỉ nhìn latency percentile.
