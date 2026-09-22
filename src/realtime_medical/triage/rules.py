
"""
Module: rules.py
 
THIẾT KẾ: Mỗi rule (heart_rate, spo2, blood_pressure...) là 1 class riêng,
kế thừa từ BaseRule và chỉ cần implement 1 hàm check(). RuleEngine chỉ có
nhiệm vụ chạy tuần tự qua danh sách rule và gộp kết quả lại.
 
MUỐN THÊM 1 RULE MỚI:
    1. Viết 1 class mới kế thừa BaseRule, implement check(self, vitals)
    2. Thêm 1 dòng khởi tạo class đó vào self.rules trong RuleEngine.__init__
    -> Không cần sửa gì trong evaluate(), không cần sửa các rule khác.
 
MUỐN XÓA 1 RULE:
    Chỉ cần xóa (hoặc comment) dòng tương ứng trong self.rules.
 
MUỐN SỬA NGƯỠNG CẢNH BÁO:
    Sửa trong config.yaml (phần triage_rules), không cần đụng vào file này.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import  List, Optional,Tuple 
from src.realtime_medical.config import TriageRulesConfig

logger = logging.getLogger(__name__)

class RiskLevel(str, Enum):
    LOW ="LOW"
    MEDIUM = "MEDIUM"
    CRITICAL = "CRITICAL"
    
    @property
    def severity(self) -> int:
        """ The higher the number the greater the risk; this is used to compare multiple rules simultaneously"""
        return {"LOW": 0, "MEDIUM":1, "CRITICAL":2}[self.value]
    
    
@dataclass
class RuleResult:
    """The result returned by a rule afer evaluating  vitals record"""
    triggered: bool
    risk_level: RiskLevel
    reason: Optional[str] = None
    

class BaseRule(ABC):
    name: str = "base_rule"
    
    @abstractmethod
    def check(self, vitals:dict) -> RuleResult:
        """Accepts a vitals record and returns a RuleResult"""
        raise NotImplementedError
    
    
class HeartRateRule(BaseRule):
    name = "heart_rate"
    
    def __init__(self, high:int, low:int):
        self.high  = high
        self.low = low
        
    def check(self,   vitals: dict) -> RuleResult:
        hr = vitals.get("hr")
        if hr is None:
            return RuleResult(False, RiskLevel.LOW)
        if hr >= self.high:
            return RuleResult(True, RiskLevel.CRITICAL, f"Heart rate so high {hr}  bpm")
        if hr <= self.low:
            return RuleResult(True, RiskLevel.CRITICAL, f"Heart rate so low {hr} bpm")
        return RuleResult(False, RiskLevel.LOW)
    
class Spo2Rule(BaseRule):
    name =  "spo2"
    def __init__(self, critical:int):
        self.critical = critical
    
    def check(self, vitals:dict):
        spo2 = vitals.get("spo2")
        if spo2 is None:
            return RuleResult(False, RiskLevel.LOW)
        if spo2 <= self.critical:
            return RuleResult(True, RiskLevel.CRITICAL,f"Spo2 drops below the threshold {spo2}%")
        return RuleResult(False, RiskLevel.LOW)
    
class BloodPressureRule(BaseRule):
    name = "blood_pressure"
    
    def  __init__(self, sbp_high: int, sbp_low: int, dbp_high:int, dbp_low: int, mbp_low:int): 
        self.sbp_high = sbp_high
        self.sbp_low  = sbp_low
        self.dbp_high =  dbp_high
        self.dbp_low = dbp_low
        self.mbp_low = mbp_low
        
    def check(self, vitals: dict) ->    RuleResult:
        sbp = vitals.get("sbp")
        dbp = vitals.get("dbp")
        mbp = vitals.get("mbp")
        if mbp is not None and mbp <= self.mbp_low:
            return RuleResult(True, RiskLevel.CRITICAL, f"Excessively low mean blood pressure ({mbp} mmHg)")
        if sbp is not None:
            if sbp >= self.sbp_high:
                return RuleResult(True, RiskLevel.CRITICAL, f"Excessively high systolic blood pressure ({sbp} mmHg)")
            if sbp <= self.sbp_low:
                return RuleResult(True, RiskLevel.CRITICAL, f"Excessively low systolic blood pressure ({sbp} mmHg)")
        if dbp is not None:
            if dbp >=  self.dbp_high:
                return RuleResult(True, RiskLevel.CRITICAL, f"Excessively high diastolic blood pressure ({dbp} mmHg)")
            if dbp <= self.dbp_low:
                return RuleResult(True, RiskLevel.CRITICAL, f"Excessively low diastolic blood pressure ({dbp} mmHg)")
        return RuleResult(False, RiskLevel.LOW) 
    
class  RespiratorRateRule(BaseRule):
    name  =  "respiratory_rate"
    
    def __init__(self, high: int, low:int) :
        self.high =  high
        self.low = low
    
    def check(self, vitals: dict) -> RuleResult:
        rr = vitals.get("rr")
        
        if rr is None:
            return RuleResult(False, RiskLevel.LOW)
        if rr >= self.high:
            return RuleResult(True, RiskLevel.CRITICAL, f"Rapid breathing ({rr} breaths/minute)")
        if  rr  <= self.low:
            return RuleResult(True, RiskLevel.CRITICAL,  f"Excessively slow respiratory rate ({rr} breaths/minute)")
        return RuleResult(False, RiskLevel.LOW)
        
class BodyTemperatureRule(BaseRule):
    name  = "body_temperature"
    
    def __init__(self, high:float, low:float):
        self.high = high
        self.low = low
        
    def check(self, vitals:dict) -> RuleResult:
        temp = vitals.get("bt")
        if temp is None:
            return  RuleResult(False, RiskLevel.LOW)
        if temp >= self.high:
            return RuleResult(True,  RiskLevel.CRITICAL,  f"High fever ({temp} °C)")
        if temp <= self.low:
            return RuleResult(True, RiskLevel.MEDIUM, f"Hypothermia ({temp} °C)")     
        return RuleResult(False, RiskLevel.LOW)
    
class ShockIndexRule(BaseRule):
    """ 
    Rule combining multiple indicators(combined rules in config.yaml)
    Shock_index = HR/SBP > threshold => suspected shock
    """
    name =  "shock_index_warning"
    
    def __init__(self, threshold:  float = 0.9):
        self.threshold = threshold
    
    def check(self, vitals: dict) -> RuleResult:
        hr = vitals.get("hr")
        sbp = vitals.get("sbp")
        if  not hr or not sbp:
            return RuleResult(False  , RiskLevel.LOW)
        
        shock_index = hr/ sbp
        if shock_index> self.threshold:
            return RuleResult(
                True,
                RiskLevel.CRITICAL,
                f"Excessively high shock index (HR/SBP  = {shock_index:.2f})"
            )
        return RuleResult(False, RiskLevel.LOW)
    
class RuleEngine:
    def __init__(self, config:TriageRulesConfig):
        self.rules: List[BaseRule] = [
            HeartRateRule(high = config.heart_rate.high, low =  config.heart_rate.low),
            Spo2Rule(critical= config.spo2.critical),
            BloodPressureRule(
                sbp_high = config.blood_pressure.sbp_high,
                sbp_low =  config.blood_pressure.sbp_low,
                dbp_high = config.blood_pressure.dbp_high,
                dbp_low = config.blood_pressure.dbp_low,
                mbp_low = config.blood_pressure.mbp_low,
            ),
            RespiratorRateRule(high = config.respiratory_rate.high, low = config.respiratory_rate.low),
            BodyTemperatureRule(high = config.body_temperature.high ,low = config.body_temperature.low),
            ShockIndexRule()
        ]
    
    def evaluate(self, vitals:dict) -> Tuple[str,List[str]]:
        """Iterate through all rules and return the highest risk level and the list  of reasons"""
        reasons:  List[str]= []
        highest_risk = RiskLevel.LOW
        
        for rule in self.rules:
            try:
                result = rule.check(vitals)
            except Exception as e:
                logger.error(f"Rule '{rule.name}' error when check: {e}")
                continue
            
            if result.triggered:
                reasons.append(result.reason)
                if result.risk_level.severity > highest_risk.severity:
                    highest_risk = result.risk_level
            
            
        return highest_risk.value, reasons



















