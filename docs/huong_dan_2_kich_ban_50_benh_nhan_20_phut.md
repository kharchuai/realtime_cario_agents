# Chạy hai kịch bản metrics với 50 case

Chuẩn bị theo [hướng dẫn PowerShell](huong_dan_chay_benchmark_powershell.md). Chọn 50 case đã seed, có CSV và tương ứng 50 bệnh nhân khác nhau; ví dụ dưới đây dùng IDs 1–50, cần kiểm tra dữ liệu thực tế trước khi chạy.

```powershell
$cases = 1..50
$env:BENCH_ENABLED = '1'
```

Baseline dùng probe critical:

```powershell
python -B scripts/run_pipeline.py --cases $cases --workers 50 --data-dir data/streaming --max-rows 1200 --realtime --critical-probes --observe-seconds 60 --verify-timeout 120
```

Chờ baseline kết thúc rồi chạy fault trên cùng nguồn:

```powershell
python -B scripts/run_pipeline.py --cases $cases --workers 50 --data-dir data/streaming --max-rows 1200 --realtime --critical-probes --observe-seconds 60 --verify-timeout 120 --fault consumer_pause --fault-at 300 --fault-duration 20
```

Consumer tự pause 20 giây tại giây nguồn 300 rồi tiếp tục. Không cần terminal thứ hai.

Lưu ý về 20 phút: source hiện tại giới hạn số hàng CSV bằng `--max-rows` và bỏ hàng trống hoặc timestamp trùng trước khi tạo probe. Vì vậy `1200` chưa bảo đảm 1200 probe/bệnh nhân hay đủ 20 phút. Muốn phát danh nghĩa 20 phút ở nhịp 1 Hz cần dữ liệu cung cấp 1200 hàng hợp lệ có timestamp riêng cho mỗi case; điều chỉnh giới hạn đọc theo CSV thực tế. Mỗi lượt còn thêm thời gian khởi động, observation, kiểm tra DB và phân tích.

Mỗi lượt in `REPORT`, đồng thời ghi đường dẫn vào runner.log:

```text
logs/<run-id>/report.json
logs/pipeline/runner.log
logs/pipeline/consumer.log
```

Trace và `db_keys.json` vẫn ở `tmp/benchmark/<run-id>/`. Baseline có `M7_M8_M9_by_fault` rỗng. Fault cần có bản ghi fault; đọc trạng thái M8 và backlog còn lại của M9 để biết đã đo đủ recovery hay chưa.
