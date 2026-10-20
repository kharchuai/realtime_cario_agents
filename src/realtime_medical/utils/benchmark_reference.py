"""Oracle ngưỡng cho benchmark replay, độc lập với RuleEngine đang kiểm thử.

Chỉ tạo nhãn critical hiện tại từ raw input; không tạo ground truth cho dự báo
ML 60 giây và không phải nhãn lâm sàng được chuyên gia xác nhận.
"""


def reference_critical(vitals, config):
    """Trả positive khi raw reading thỏa ít nhất một ngưỡng critical đã khai báo.

    Gọi trước fault drop-field. Không dựa vào trace/kết quả consumer để tạo
    mẫu số. BT thấp là MEDIUM trong baseline hiện tại, không thuộc cohort này.
    """
    rules = config.triage_rules
    thresholds = (
        ("hr", rules.heart_rate.low, rules.heart_rate.high),
        ("sbp", rules.blood_pressure.sbp_low, rules.blood_pressure.sbp_high),
        ("dbp", rules.blood_pressure.dbp_low, rules.blood_pressure.dbp_high),
        ("rr", rules.respiratory_rate.low, rules.respiratory_rate.high),
    )
    for field, low, high in thresholds:
        value = vitals.get(field)
        if value is not None and (value <= low or value >= high):
            return True
    for field, threshold in (("spo2", rules.spo2.critical), ("mbp", rules.blood_pressure.mbp_low)):
        value = vitals.get(field)
        if value is not None and value <= threshold:
            return True
    if vitals.get("bt") is not None and vitals["bt"] >= rules.body_temperature.high:
        return True
    hr, sbp = vitals.get("hr"), vitals.get("sbp")
    # Baseline đang khởi tạo ShockIndexRule mặc định 0.9; ghi rõ hạn chế này.
    return bool(hr and sbp and hr / sbp > 0.9)
