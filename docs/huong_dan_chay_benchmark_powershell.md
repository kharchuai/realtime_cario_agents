# Hướng dẫn chạy baseline và fault bằng PowerShell

Áp dụng cho source hiện tại tại `E:\dowload\Serminal\realtime_cario_agents_codex`. Các lệnh dưới đây dùng PowerShell, không phải CMD hay Bash. Bạn có thể copy từng block theo thứ tự.

Cập nhật 16/10/2026: suite đã hỗ trợ duration fault và replay CSV thật. Các ví dụ cũ phía dưới vẫn dùng mặc định probe, pause 20s và bỏ HR 20s. Để đo dữ liệu mới với RR_CO2 và fault ngắn, dùng:

```powershell
$suiteId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_rrco2_real40'
python -B scripts/run_benchmark_suite.py --suite-id $suiteId --patients 40 --ticks 120 --source-mode replay --data-dir data/streaming --window-anchor rr --pause-seconds 10 --gap-seconds 10 --drop-field rr
```

`--window-anchor rr` chọn cửa sổ từ RR hợp lệ đầu tiên của từng case, fallback từ reading đầu nếu không có RR. CSV nguồn không bị sửa. Replay giữ các giá trị thiếu và bỏ hàng toàn trống như pipeline thường. Muốn kiểm tra đầu ca thay vì chọn cửa sổ có RR, dùng `--window-anchor first_valid`. RTO theo analyzer cần probe đều; chạy thêm một suite `--source-mode probes --scenarios baseline consumer_pause --ticks 100 --pause-seconds 10 --skip-build`, tuần tự sau suite dữ liệu thật. Mất ACK là lỗi một lần sau commit, không phải fault kéo dài 10s.

## 1. Phân biệt các chế độ

| Chế độ/kịch bản | Có đo? | Nguồn và hành vi | Mục đích |
|---|---|---|---|
| Pipeline thuần | Không, `BENCH_ENABLED=0` | Xử lý dữ liệu thường, không yêu cầu `_bench` | Kiểm tra chạy ứng dụng không cần benchmark |
| `baseline` | Có | Không tạo fault; fixture đủ 7 fields, SpO2=85 | Đo khi pipeline hoạt động bình thường |
| `consumer_pause` | Có | Pause consumer 20s ở khoảng giây 35; nguồn vẫn phát | Critical-path continuity, RTO, backlog drain |
| `ack_loss` | Có | Lần lưu batch alert đầu tiên commit thật rồi giả mất ACK | Alert persistence latency, alert trùng khi retry |
| `signal_gap` | Có | Bỏ HR tại ticks 30–49, giữ các trường khác | Raw data completeness, signal gap/age |

**Baseline không có fault vẫn dùng dữ liệu critical tổng hợp.** “Bình thường” ở đây nghĩa là hệ thống không bị gây lỗi, không phải bệnh nhân có sinh hiệu bình thường. Mỗi reading fixture kỳ vọng có một alert do SpO2 thấp.

Các mục 2–7 dùng script tự động `scripts/run_benchmark_suite.py`, chỉ cần **một cửa sổ PowerShell**. Mục 8 dành cho điều khiển thủ công.

## 2. Chuẩn bị trước khi chạy

Mở Docker Desktop, rồi mở PowerShell:

```powershell
Set-Location 'E:\dowload\Serminal\realtime_cario_agents_codex'

python --version
docker version

$env:PYTHONIOENCODING = 'utf-8'
$env:POSTGRES_HOST = 'localhost'
$env:POSTGRES_PORT = '5433'
$env:KAFKA_BROKER_URL = 'localhost:9092'
$env:ML_INFERENCE_N_JOBS = '1'

# Đưa môi trường cửa sổ này về chế độ thường trước khi bắt đầu.
$env:BENCH_ENABLED = '0'
$env:BENCH_ALERT_ACK_LOSS = '0'
$env:BENCH_CRITICAL_PROBES = '0'
$env:BENCH_DROP_FIELD = ''
Remove-Item Env:KAFKA_TOPIC_VITALS -ErrorAction SilentlyContinue

docker compose up -d postgres zookeeper kafka kafka-init
docker compose ps -a
docker compose build consumer
```

`Remove-Item Env:...` chỉ gỡ biến môi trường của cửa sổ PowerShell, không xóa file. Bỏ override topic để Compose sử dụng topic cấu hình trong `.env`, mặc định là `patient_vitals_stream`.

Kiểm tra dependencies của Python host:

```powershell
python -B -c "import pandas, sqlalchemy, confluent_kafka, pydantic_settings, yaml; print('Dependencies OK')"
```

Máy này đã chạy được benchmark ở lượt trước nên thường không cần cài lại. Nếu dựng môi trường mới, dùng Python 3.11 cho các phiên bản trong `requirements.txt`; không mặc định rằng những bản pin này cài được trên mọi phiên bản Python.

Điều kiện dữ liệu DB:

- `--patients 40` dùng surgery IDs **1–40**, thuộc 40 bệnh nhân khác nhau.
- `--patients 30` dùng 1–30; `--patients 50` dùng 1–50.
- Những surgery/patient này phải có sẵn trong DB. Script sẽ kiểm tra trước khi phát nguồn.
- User/password/database lấy từ `.env` hiện có. Không cần ghi mật khẩu vào lệnh.

Chỉ chạy **một suite tại một thời điểm**: các lượt cùng điều khiển container `medical_consumer`. Nếu đang có producer khác phục vụ công việc của bạn, dừng producer đó trước vì suite sẽ đổi consumer sang topic benchmark.

## 3. Chạy cả bốn kịch bản — cách dùng đầy đủ

Sau khi hoàn tất mục 2:

```powershell
$suiteId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_40patients'

python -B scripts/run_benchmark_suite.py --suite-id $suiteId --patients 40 --skip-build

if ($LASTEXITCODE -ne 0) { throw 'Benchmark chưa hoàn tất; kiểm tra log của lượt chạy.' }
```

Script lần lượt chạy `baseline`, `consumer_pause`, `ack_loss`, `signal_gap`. Bạn không phải mở cửa sổ khác để bấm pause hoặc đặt các biến `BENCH_*` thủ công.

Script tự làm các việc sau:

1. Tạo CSV tổng hợp riêng, đủ 7 fields; không sửa CSV nguồn của bạn.
2. Bật metrics, tạo run ID/topic riêng cho mỗi kịch bản, cấu hình consumer và chờ Kafka assign partition.
3. Phát đồng thời 40 bệnh nhân, mỗi bệnh nhân 1 reading/giây; inference dùng một thread.
4. Tạo fault theo kịch bản, khôi phục pause, chờ quan sát và kiểm tra DB.
5. Dừng consumer benchmark, đóng collector, chạy analyzer với kiểm tra alert trùng trong DB và khoảng throughput.
6. Kiểm tra pipeline thuần bằng 5 readings/bệnh nhân với `--realtime`, sau đó chạy lại consumer với metrics tắt.

Mặc định baseline/pause có 100 readings/bệnh nhân; ACK loss/signal gap có 70. Ví dụ 100 ticks tương ứng mốc 0–99s từ reading đầu đến reading cuối. Tổng thời gian còn gồm startup, chờ DB, quan sát và kiểm tra chế độ thường; có thể mất nhiều phút.

`--skip-build` chỉ dùng nếu bạn vừa build hoặc source/dependencies chưa thay đổi. Nếu đã sửa code, chạy lại `docker compose build consumer` hoặc bỏ `--skip-build` để script build trước khi đo.

Run ID cần mới cho mỗi lượt. Không dùng lại ID cũ: collector cố ý không ghi đè trace.

## 4. Chạy riêng từng kịch bản

Các block sau độc lập; chạy block bạn cần sau mục 2. Mỗi lệnh vẫn tự kiểm tra chế độ thường sau khi đo xong.

### 4.1. Baseline: có đo metrics, không tạo fault

```powershell
$baselineId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_baseline40'
python -B scripts/run_benchmark_suite.py --suite-id $baselineId --patients 40 --scenarios baseline --skip-build
```

Kết quả nằm ở `test/benchmark_results/<baselineId>_baseline/report.json`. M1–M6 và M10 có dữ liệu; danh sách fault M7/M8/M9 rỗng là đúng vì lượt này không có pause.

### 4.2. Pause consumer

```powershell
$pauseId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_pause40'
python -B scripts/run_benchmark_suite.py --suite-id $pauseId --patients 40 --scenarios consumer_pause --skip-build
```

Ở khoảng giây 35, script pause `medical_consumer` trong 20s rồi unpause. Kafka vẫn nhận readings từ nguồn; consumer không xử lý trong lúc bị pause.

Xem `M7_M8_M9_by_fault` trong `report.json`: detection/alert đúng hạn trong fault, RTO từng luồng, backlog và drain time. `not_recovered_within_observation` nghĩa là chưa xác nhận đủ 5 vòng probe tốt liên tiếp; không phải RTO=0.

Để đo recovery với tải thấp hơn và nguồn chạy lâu hơn:

```powershell
$recoveryId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_recovery30'
python -B scripts/run_benchmark_suite.py --suite-id $recoveryId --patients 30 --ticks 160 --scenarios consumer_pause --skip-build
```

Thay số bệnh nhân sẽ thay tải thử nghiệm; không ghép RTO ở 30 bệnh nhân thành kết quả của lượt 40 bệnh nhân.

### 4.3. Giả mất ACK sau commit alert

```powershell
$ackId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_ack40'
python -B scripts/run_benchmark_suite.py --suite-id $ackId --patients 40 --scenarios ack_loss --skip-build
```

Đây là fault trong process consumer: DB ghi alert thành công nhưng caller nhận exception một lần, rồi retry theo logic hiện tại. Bạn không cần ngắt mạng hay tắt PostgreSQL.

Xem M5 và M6. Với cơ chế lưu hiện tại, retry có thể tạo alert trùng. Không chỉ đếm số trace retry; M6 đối chiếu dòng vật lý trong DB.

### 4.4. Thiếu HR trong 20 ticks

```powershell
$gapId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_gap40'
python -B scripts/run_benchmark_suite.py --suite-id $gapId --patients 40 --scenarios signal_gap --skip-build
```

Nguồn vẫn phát readings nhưng bỏ field `hr` tại ticks 30–49. Tick 50 gửi HR trở lại. SpO2 vẫn thấp nên expectation alert critical vẫn có hiệu lực.

Xem M1 `fields` và M2 `streams` theo `field='hr'`. `max_active_receive_gap_s`/`max_active_signal_age_s` chỉ xét khi nguồn còn hoạt động. Forward-fill trong ML không được tính là một raw HR sample mới.

Với 40 bệnh nhân, 70 readings/người, fault bỏ 20 HR/người thì thiếu đúng 800 HR trên 19.600 trường kỳ vọng. Nếu tất cả reading lưu đúng hạn, raw field completeness kỳ vọng là khoảng 95,92%.

### 4.5. Đổi tải hoặc thời gian phát

```powershell
$loadId = 'bench_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '_load50'
python -B scripts/run_benchmark_suite.py --suite-id $loadId --patients 50 --ticks 120 --scenarios baseline --skip-build
```

Script hiện nhận 30–50 bệnh nhân; `--ticks` nếu đặt phải ≥70. Số worker bằng số bệnh nhân. `--ticks` đổi số hàng/thời gian phát; pause bắt đầu khoảng giây 35 và drop bắt đầu giây 30. Dùng `--pause-seconds`, `--gap-seconds`, `--drop-field` để đổi duration và sinh hiệu bị bỏ.

Suite ghi đè các biến fault theo CLI. Đặt `$env:BENCH_DROP_START_TICK` ngoài cửa sổ sẽ không đổi mốc bắt đầu của suite. Muốn điều khiển mốc bắt đầu thủ công, xem mục 8.

## 5. Xem kết quả và log

Ví dụ sau lượt chạy đầy đủ ở mục 3, biến `$suiteId` còn trong cùng cửa sổ:

```powershell
$runId = $suiteId + '_baseline'
$runDir = Join-Path (Get-Location).Path ('test\benchmark_results\' + $runId)
$report = Get-Content -LiteralPath (Join-Path $runDir 'report.json') -Raw -Encoding UTF8 | ConvertFrom-Json

$report.M3 | ConvertTo-Json -Depth 10
$report.M4 | ConvertTo-Json -Depth 10
$report.M6 | ConvertTo-Json -Depth 10
$report.M10 | ConvertTo-Json -Depth 10

Get-Content -LiteralPath (Join-Path $runDir 'runner.log') -Tail 30 -Encoding UTF8
Get-Content -LiteralPath (Join-Path $runDir 'consumer.log') -Tail 30 -Encoding UTF8
```

Đổi suffix `_baseline` thành `_consumer_pause`, `_ack_loss` hoặc `_signal_gap`. Nếu chạy riêng, dùng biến tương ứng: ví dụ `$runId = $ackId + '_ack_loss'`.

Đọc recovery của lượt pause:

```powershell
$runId = $pauseId + '_consumer_pause'
$reportPath = Join-Path (Get-Location).Path ('test\benchmark_results\' + $runId + '\report.json')
$report = Get-Content -LiteralPath $reportPath -Raw -Encoding UTF8 | ConvertFrom-Json
$report.M7_M8_M9_by_fault | ConvertTo-Json -Depth 15
```

Mỗi run có `report.json`, `scenario.json`, `db_keys.json`, JSONL, health sidecar, `runner.log`, `consumer.log`, startup/stop/analyzer logs. Thư mục suite không có suffix lưu fixtures, `suite.json` và kết quả kiểm tra chế độ thường.

Trong lúc suite đang chạy, dùng **cửa sổ PowerShell thứ hai** để xem consumer mà không tạo fault thêm:

```powershell
Set-Location 'E:\dowload\Serminal\realtime_cario_agents_codex'
docker compose logs -f --tail 30 consumer
```

Ctrl+C ở cửa sổ xem log chỉ thoát lệnh xem log. Không dùng Ctrl+C ở cửa sổ đang chạy suite nếu muốn hoàn tất lượt đo.

## 6. Xuất thống kê thành Markdown

Sau khi suite đã báo `SUITE COMPLETE` (một hoặc nhiều kịch bản đều được):

```powershell
$outputMd = 'docs/ket_qua_' + $suiteId + '.md'
python -B scripts/summarize_benchmark.py --suite-id $suiteId --check-db --output $outputMd
```

Nếu đã chạy thêm lượt recovery 30 bệnh nhân ở mục 4.2:

```powershell
$outputMd = 'docs/ket_qua_' + $suiteId + '.md'
python -B scripts/summarize_benchmark.py --suite-id $suiteId --supplement-id $recoveryId --check-db --output $outputMd
```

Script ghi file `.md` và file `.json` cùng tên. Markdown chỉ có bảng số liệu; JSON giữ các counts/status chi tiết. `--check-db` đọc DB theo manifest và đưa đối chiếu vào output, không sửa DB. Có thể bỏ `--output` để lưu `summary.md` và `summary.json` ngay trong thư mục suite. Chạy sau khi các writer benchmark đã dừng.

Summarizer hiện đọc thông số từ kết quả chạy, dùng được cho replay/probe, thay đổi tải, duration và số kịch bản. Nó không viết nhận xét hoặc báo cáo theo form.

Để kiểm tra trace, hash nguồn và DB:

```powershell
python -B scripts/review_benchmark.py --suite-id $suiteId --check-db
```

Kết quả lưu trong `test/benchmark_results/<suite-id>/checks.json`. Bỏ `--check-db` để chỉ kiểm tra file/trace khi không kết nối DB. Dùng `--supplement-id $recoveryId` nếu cần thêm suite recovery. Review không còn nhận `--real-suite`, `--probe-suite`, `--form`; dùng `--suite-id` và `--supplement-id`. Các lệnh trong báo cáo ngày 16/10 mô tả phiên bản script tại lúc đo.

## 7. Chạy pipeline thuần, không đo metrics

Suite tự quay về chế độ này khi xong. Để thiết lập thủ công trong cửa sổ PowerShell:

```powershell
Set-Location 'E:\dowload\Serminal\realtime_cario_agents_codex'

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
Remove-Item Env:BENCH_CASES -ErrorAction SilentlyContinue
Remove-Item Env:KAFKA_TOPIC_VITALS -ErrorAction SilentlyContinue

docker compose up -d --no-deps --force-recreate consumer
docker compose logs --tail 30 consumer
```

Đợi log có `KAFKA_ASSIGNED` trước khi phát nguồn. `--force-recreate` cần thiết để cập nhật environment của container; chỉ đổi `$env:...` trên host không làm consumer đang chạy đổi chế độ. `docker compose restart consumer` cũng không cập nhật environment theo cấu hình mới.

Ví dụ kiểm tra nhanh 40 bệnh nhân bằng fixture của suite đã chạy ở mục 3:

```powershell
$fixtureDir = Join-Path (Get-Location).Path ('test\benchmark_results\' + $suiteId + '\fixtures')
$caseIds = 1..40

python -B scripts/run_pipeline.py --cases $caseIds --workers 40 --data-dir $fixtureDir --max-rows 5 --realtime --verify-timeout 90
```

Nếu mở cửa sổ mới, `$suiteId` không còn: gán lại bằng ID thư mục suite có sẵn. Trên máy này có thể dùng:

```powershell
$suiteId = 'bench_20261014_40patients_v2'
```

Fixture vẫn có SpO2 thấp nên vẫn sinh alert; chỉ instrumentation được tắt. Muốn phát CSV nguồn thực của bạn, thay `--data-dir` bằng đường dẫn `data/streaming`, chọn cases/số rows phù hợp. CSV thật có thể có hàng trống hoặc các mốc `time_sec` cách xa; thời gian chạy và số readings hợp lệ sẽ khác fixture.

Khi `BENCH_ENABLED=0`, không cần run ID, metrics directory hay `_bench` trong payload. Producer/consumer không tạo collector output cho lượt này; không cần sửa code để chuyển chế độ.

## 8. Điều khiển fault thủ công bằng hai cửa sổ

Phần này độc lập với suite tự động. Không chạy cùng lúc với `run_benchmark_suite.py`. Dùng khi bạn muốn chọn chính xác target/duration pause hoặc khoảng bỏ tín hiệu.

### 8.1. Cửa sổ A: chuẩn bị nguồn và consumer benchmark

Sau khi hạ tầng đã sẵn sàng và image đã build ở mục 2:

```powershell
Set-Location 'E:\dowload\Serminal\realtime_cario_agents_codex'

$env:PYTHONIOENCODING = 'utf-8'
$env:POSTGRES_HOST = 'localhost'
$env:POSTGRES_PORT = '5433'
$env:KAFKA_BROKER_URL = 'localhost:9092'
$env:ML_INFERENCE_N_JOBS = '1'

$env:BENCH_ENABLED = '1'
$env:BENCH_RUN_ID = 'manual_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
$env:METRICS_DIR = Join-Path (Get-Location).Path 'test/benchmark_results'
$env:BENCH_CASES = (1..40) -join ','
$env:BENCH_SIGNAL_FIELDS = 'hr,spo2,sbp,dbp,mbp,rr,bt'
$env:BENCH_CRITICAL_PROBES = '1'
$env:KAFKA_TOPIC_VITALS = $env:BENCH_RUN_ID.ToLowerInvariant()

# Baseline hoặc pause: giữ 0 và field rỗng.
$env:BENCH_ALERT_ACK_LOSS = '0'
$env:BENCH_DROP_FIELD = ''
$env:BENCH_DROP_START_TICK = '30'
$env:BENCH_DROP_END_TICK = '50'

New-Item -ItemType Directory -Path $env:METRICS_DIR -Force | Out-Null
python -B -c "from pathlib import Path; from scripts.run_benchmark_suite import write_fixtures; write_fixtures(Path('test/benchmark_results/manual_fixtures'), list(range(1,41)),180)"

# Chia sẻ đúng run ID sang cửa sổ B, không ghi thông tin đăng nhập DB.
$sessionInfo = @{
    run_id = $env:BENCH_RUN_ID
    metrics_dir = $env:METRICS_DIR
    topic = $env:KAFKA_TOPIC_VITALS
}
$sessionPath = Join-Path $env:METRICS_DIR 'manual_session.json'
[System.IO.File]::WriteAllText($sessionPath, ($sessionInfo | ConvertTo-Json), [System.Text.UTF8Encoding]::new($false))

docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic $env:KAFKA_TOPIC_VITALS --partitions 3 --replication-factor 1
docker compose up -d --no-deps --force-recreate consumer

# Chờ Kafka assignment, tối đa 90s.
$ready = $false
$readyUntil = (Get-Date).AddSeconds(90)
while ((Get-Date) -lt $readyUntil) {
    $consumerLog = docker compose logs --no-color --tail 100 consumer
    if ($consumerLog -match 'KAFKA_ASSIGNED') {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 1
}
if (-not $ready) { throw 'Consumer chưa được assign; xem docker compose logs consumer.' }

$caseIds = 1..40
python -B scripts/run_pipeline.py --cases $caseIds --workers 40 --data-dir test/benchmark_results/manual_fixtures --max-rows 180 --observe-seconds 15 --verify-timeout 120
```

Để chạy **baseline thủ công**, chỉ để nguồn chạy xong, không thực hiện lệnh tạo fault ở cửa sổ B. Muốn pause, mở cửa sổ B trong khi A đang phát readings. Run ID/metrics directory của controller phải trùng nguồn và consumer; file session ở trên giúp tránh nhập nhầm.

### 8.2. Cửa sổ B: pause consumer trong 20s

Chạy khi cửa sổ A đã bắt đầu phát readings và nguồn còn hoạt động:

```powershell
Set-Location 'E:\dowload\Serminal\realtime_cario_agents_codex'

$session = Get-Content -LiteralPath 'test/benchmark_results/manual_session.json' -Raw -Encoding UTF8 | ConvertFrom-Json
$env:BENCH_ENABLED = '1'
$env:BENCH_RUN_ID = $session.run_id
$env:METRICS_DIR = $session.metrics_dir

python -B scripts/run_fault.py --container medical_consumer --duration-s 20
```

Lệnh tạo fault ngay khi được gọi, không tự chờ giây 35. Controller ghi `fault_applied`, chờ 20s, unpause và ghi `fault_restored`; nguồn ở A vẫn chạy. Muốn đúng lịch giây 35 như lượt đo trước, dùng suite tự động.

Target/duration khác cũng được CLI hỗ trợ. Trong **một lượt thủ công mới**, sau khi nạp session như trên, chọn một lệnh:

```powershell
# PostgreSQL tạm dừng 10s.
python -B scripts/run_fault.py --container medical_postgres --duration-s 10

# Hoặc Kafka tạm dừng 10s.
python -B scripts/run_fault.py --container medical_kafka --duration-s 10
```

Đây là các kịch bản mở rộng mà CLI hỗ trợ; lượt đo trước chỉ thực nghiệm pause consumer. Nên dùng run mới cho mỗi target để dễ quy nguyên nhân. Fault Kafka có thể làm việc gửi nguồn chậm; fault DB có thể chặn xử lý consumer. Không tự suy ra chúng có cùng RTO như pause consumer.

### 8.3. Tạo ACK loss hoặc thiếu HR thủ công

Với **run mới**, tại block thiết lập cửa sổ A, thay các biến trước lệnh recreate consumer/phát nguồn:

```powershell
# ACK loss: thay dòng gán 0 thành 1; không cần run_fault.py.
$env:BENCH_ALERT_ACK_LOSS = '1'
```

ACK fault được cài khi consumer khởi động và xảy ra một lần ở batch alert đầu tiên. Đổi biến sau khi consumer đã chạy sẽ chưa có tác dụng; cần recreate consumer với environment mới.

Hoặc giữ ACK loss bằng 0 và dùng:

```powershell
# Bỏ HR trong ticks 20–39; tick 40 gửi lại.
$env:BENCH_DROP_FIELD = 'hr'
$env:BENCH_DROP_START_TICK = '20'
$env:BENCH_DROP_END_TICK = '40'
```

Các biến drop field được runner trên host đọc khi phát. Không cần truyền chúng vào consumer. Chỉ hoạt động với benchmark probe đang bật. Dùng HR để vẫn giữ SpO2 critical; bỏ SpO2 mà vẫn kỳ vọng critical theo fixture hiện tại sẽ làm oracle không còn phù hợp.

### 8.4. Sau khi nguồn ở A chạy xong: đóng writer và phân tích

Thực hiện trong cửa sổ A, giữ nguyên run ID và metrics directory:

```powershell
docker compose stop -t 30 consumer

$runDir = Join-Path $env:METRICS_DIR $env:BENCH_RUN_ID
$runInfoJson = python -B -c "import json, os; from pathlib import Path; from src.realtime_medical.utils.benchmark_analysis import load_run; d=load_run(Path(os.environ['METRICS_DIR']) / os.environ['BENCH_RUN_ID'], os.environ['BENCH_RUN_ID']); t=min(e['t0_ns'] for e in d['expected_vitals'].values()); print(json.dumps({'start':t+10_000_000_000,'end':t+170_000_000_000}))"
if ($LASTEXITCODE -ne 0) { throw 'Không đọc được trace của run.' }
$runInfo = $runInfoJson | ConvertFrom-Json

$reportJson = python -B scripts/analyze_benchmark.py --run-id $env:BENCH_RUN_ID --metrics-dir $env:METRICS_DIR --check-db --steady-start-ns $runInfo.start --steady-end-ns $runInfo.end
if ($LASTEXITCODE -ne 0) { throw 'Analyzer thất bại; chưa ghi report.json.' }

# UTF-8 không BOM, đọc được bằng json.loads trong các script Python.
[System.IO.File]::WriteAllText((Join-Path $runDir 'report.json'), ($reportJson -join "`n"), [System.Text.UTF8Encoding]::new($false))

Get-Content -LiteralPath (Join-Path $runDir 'report.json') -Raw -Encoding UTF8
```

Khoảng throughput ở đây là giây 10–170, phù hợp nguồn 180 ticks trong block A. Nếu bạn đổi số ticks, phải đổi mốc end tương ứng; nếu khoảng chứa fault thì đây là throughput trong lỗi/phục hồi, không gọi là steady-state khỏe.

Runner tự ghi `observation_ended` khi kết thúc; không thêm marker này bằng tay. Dừng consumer trước analyzer để collector đóng và DB không còn thay đổi trong lượt. Sau khi đọc kết quả, chạy mục 7 để khôi phục consumer thường. Cửa sổ B có environment riêng; nếu sau đó chạy producer ở B thì cũng phải tắt các biến benchmark ở B.

## 9. Dừng lỗi và khôi phục khi thao tác bị gián đoạn

Kiểm tra trạng thái pause:

```powershell
docker inspect --format '{{.State.Paused}}' medical_consumer
docker inspect --format '{{.State.Paused}}' medical_postgres
docker inspect --format '{{.State.Paused}}' medical_kafka
```

Nếu container nào trả `true`, unpause đúng container đó. Ví dụ:

```powershell
docker unpause medical_consumer
```

Sau đó khôi phục theo mục 7. Lượt bị ngắt có thể thiếu `observation_ended`, sidecar chưa closed hoặc nguồn phát chưa đủ; không sử dụng nó như một lượt đo hoàn chỉnh. Tạo run ID mới để chạy lại.

Nếu `report.json` chưa xuất hiện, xem `runner.log`, `startup.log`, `consumer.log` hoặc log live. Kiểm tra thêm:

```powershell
docker compose ps -a
docker compose logs --tail 50 consumer
```

Các file kết quả nằm trong `test/benchmark_results/` của dự án và được Git ignore. Chúng vẫn tồn tại trên máy; cần giữ lại nếu muốn đối chiếu báo cáo sau này.
