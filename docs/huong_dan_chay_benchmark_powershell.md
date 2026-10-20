# Chạy baseline và fault có metrics (PowerShell)

Chạy tại thư mục gốc dự án, sau khi cài dependencies và cấu hình `.env`. Chuẩn bị hạ tầng và seed dữ liệu nếu chưa có:

```powershell
docker compose up -d postgres zookeeper kafka kafka-init
docker compose build consumer
$env:POSTGRES_HOST = 'localhost'
$env:POSTGRES_PORT = '5433'
$env:KAFKA_BROKER_URL = 'localhost:9092'
python -B -m scripts.seed_data_clinical --patients 50
$env:BENCH_ENABLED = '1'
```

Chờ hạ tầng sẵn sàng trước khi seed. Chỉ cần build lại consumer khi source/model/config đổi. Các case phải có trong DB và có CSV tương ứng.

Baseline:

```powershell
python -B scripts/run_pipeline.py --cases 1 2 3 --workers 3 --data-dir data/streaming --max-rows 1200 --realtime --verify-timeout 120
```

Fault (chạy sau khi baseline kết thúc):

```powershell
python -B scripts/run_pipeline.py --cases 1 2 3 --workers 3 --data-dir data/streaming --max-rows 1200 --realtime --verify-timeout 120 --critical-probes --fault consumer_pause --fault-at 300 --fault-duration 20
```

Fault pause consumer 20 giây tại giây nguồn 300 rồi tự unpause. `--critical-probes` thay giá trị sinh hiệu bằng probe critical theo nhịp 1 giây; để so sánh cùng nguồn, thêm cờ này vào lệnh baseline. Muốn replay giá trị CSV gốc ở cả hai lượt thì bỏ cờ ở fault; M8 cần đủ probe recovery để kết luận.

Runner tự tạo run ID, khởi động consumer, chờ sẵn sàng, phát dữ liệu, dừng consumer và xuất report. Không cần mở consumer riêng hay chạy analyzer sau đó. Nguồn fault phải dài hơn 320 giây và còn đủ dữ liệu phục hồi. `--max-rows` là số hàng CSV, không phải số giây hoặc số mẫu hợp lệ.

Kết quả:

```text
logs/pipeline/runner.log
logs/pipeline/consumer.log
logs/<run-id>/report.json
tmp/benchmark/<run-id>/  # trace, health và db_keys.json
```

Log được ghi nối tiếp, kể cả khi bật metrics. Đường dẫn `REPORT` và `RUN_ID` có trong runner.log. Các lượt mới không dùng `test/benchmark_results`; dữ liệu suite cũ nếu có vẫn giữ nguyên.

Baseline không có fault nên `M7_M8_M9_by_fault` rỗng. Ở lượt fault, đọc trạng thái M7/M8/M9 cùng `execution`, `telemetry_issues` và backlog còn lại; có report không đồng nghĩa mọi metric đã đo đủ hoặc đạt yêu cầu.
