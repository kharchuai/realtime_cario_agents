import unittest

import pandas as pd

from src.realtime_medical.config import AppConfig
from src.realtime_medical.streaming.windowing import (
    CANONICAL_FIELDS,
    WindowAggregator,
    build_offline_windows,
)

class WindowingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = AppConfig.load_config("configs/config.yaml")
        cls.streaming_config = cls.config.streaming

    @staticmethod
    def make_vitals(**overrides):
        vitals = {
            "hr": 80,
            "spo2": 98,
            "sbp": 120,
            "dbp": 80,
            "mbp": 93,
            "rr": 16,
            "bt": 37.0,
        }
        vitals.update(overrides)
        return vitals

    def make_dataframe(self, sample_count= 6, **overrides):
        rows =[]

        for time_sec in range(sample_count):
            row =  {
                "time_sec": float(time_sec),
                **self.make_vitals(**overrides),
            }
            rows.append(row)

        return pd.DataFrame(rows)

    def test_canonical_fields_are_available(self):
        expected_field = {
            "hr",
            "spo2",
            "sbp",
            "dbp",
            "mbp",
            "rr",
            "bt",
        }
        self.assertEqual(set(CANONICAL_FIELDS), expected_field)

    def test_not_enough_samples_does_not_create_window(self):
        aggregator = WindowAggregator(self.streaming_config)

        results = []

        sample_count = self.streaming_config.min_samples_to_evaluate - 1

        for timestamp in range(sample_count):
            result = aggregator.add_reading(
                surgery_id = 1, 
                vitals =  self.make_vitals  (),
                timestamp = float(timestamp),
            )
            results.append(result)

        self.assertTrue(all(result is None for result in results))

    def test_enough_samples_create_window_feature(self):
        dataframe = self.make_dataframe(
            sample_count = self.streaming_config.min_samples_to_evaluate + 1,
            hr = 80,
        )

        windows = build_offline_windows(
            dataframe, 
            config =  self.streaming_config,
            surgery_id = 1,
        )

        self.assertFalse(windows.empty)
        self.assertIn("surgery_id", windows.columns)
        self.assertIn("hr_mean", windows.columns)

        self.assertTrue((windows["surgery_id"] == 1).all())
        self.assertTrue((windows["hr_mean"] == 80).all())

    def test_constant_values_produce_expected_mean(self):
        dataframe = self.make_dataframe(
            sample_count = self.streaming_config.min_samples_to_evaluate + 1,
            hr = 75,
            spo2 = 97,
            sbp =115,
            dbp = 70,
            mbp = 85,
            rr = 14,
            bt = 36.5,
        )

        windows =  build_offline_windows(
            dataframe,
            config  = self.streaming_config,
            surgery_id =10,
        )

        self.assertFalse(windows.empty)

        last_window = windows.iloc[-1]

        expected_means = {
            "hr_mean": 75,
            "spo2_mean":97,
            "sbp_mean":115,
            "dbp_mean": 70,
            "mbp_mean": 85,
            "rr_mean": 14,
            "bt_mean": 36.5, 
        }

        for feature_name, expected_value in expected_means.items():
            with self.subTest(feature = feature_name):
                self.assertIn(feature_name,  windows.columns)
                self.assertAlmostEqual(
                    float(last_window[feature_name]),
                    expected_value,
                )
        
    def test_each_surgery_uses_an_independent_buffer(self):
        aggregator = WindowAggregator(self.streaming_config)

        surgery_1_results = []
        surgery_2_results = []

        sample_count = self.streaming_config.min_samples_to_evaluate + 1
        for timestamp in range(sample_count):
            surgery_1_results.append(
                aggregator.add_reading(
                    surgery_id = 1,
                    vitals = self.make_vitals(hr = 70),
                    timestamp = float(timestamp),
                )
            )

            surgery_2_results.append(
                aggregator.add_reading(
                    surgery_id = 2,
                    vitals = self.make_vitals(hr = 110),
                    timestamp = float(timestamp),
                )                
            )

        surgery_1_windows = [
            result for result in surgery_1_results if result is not None
        ]

        surgery_2_windows = [
            result for result in surgery_2_results if result is not None
        ]

        self.assertGreaterEqual(len(surgery_1_windows), 1)
        self.assertGreaterEqual(len(surgery_2_windows), 1)

        self.assertEqual(surgery_1_windows[-1]["surgery_id"], 1)
        self.assertEqual(surgery_2_windows[-1]["surgery_id"], 2)

        self.assertAlmostEqual(float(surgery_1_windows[-1]["hr_mean"]), 70)
        self.assertAlmostEqual(float(surgery_2_windows[-1]["hr_mean"]), 110)

    def test_offline_windowing_is_deterministic(self):
        dataframe = self.make_dataframe(
            sample_count = 12,
            hr =80,
            spo2 =96,
        )

        first_result = build_offline_windows(
            dataframe.copy(),
            config = self.streaming_config,
            surgery_id = 5,
        )

        second_result = build_offline_windows(
            dataframe.copy(),
            config = self.streaming_config,
            surgery_id = 5,
        )

        pd.testing.assert_frame_equal(
            first_result.reset_index(drop = True),
            second_result.reset_index(drop = True),
        )

    def test_input_rows_are_sorted_by_time(self):
        dataframe = self.make_dataframe(
            sample_count = self.streaming_config.min_samples_to_evaluate +1 ,
            hr = 80.
        )

        shuffled = dataframe.sample(frac =1 , random_state = 42).reset_index(drop = True)

        ordered_result = build_offline_windows(
            dataframe, 
            config = self.streaming_config,
            surgery_id = 7,
        )

        shuffled_result = build_offline_windows(
            shuffled, 
            config = self.streaming_config,
            surgery_id = 7,
        )

        pd.testing.assert_frame_equal(
            ordered_result.reset_index(drop = True),
            shuffled_result.reset_index(drop = True),
        )

    def test_missing_measurements_do_not_crash_windowing(self):
        dataframe = self.make_dataframe(
            sample_count = self.streaming_config.min_samples_to_evaluate + 1
        )

        dataframe.loc[1, "hr"] = None
        dataframe.loc[2, "spo2"] = None
        dataframe.loc[3, "bt"] =None

        windows = build_offline_windows(
            dataframe, 
            config =self.streaming_config,
            surgery_id = 20,
        )

        self.assertFalse(windows.empty)
        self.assertIn("hr_mean", windows.columns)
        self.assertIn("spo2_mean", windows.columns)
        self.assertIn("bt_mean",  windows.columns)

        last_window = windows.iloc[-1]

        self.assertFalse(pd.isna(last_window["hr_mean"]))
        self.assertFalse(pd.isna(last_window["spo2_mean"]))
        self.assertFalse(pd.isna(last_window["bt_mean"]))

if __name__ == "__main__":
    unittest.main()

    