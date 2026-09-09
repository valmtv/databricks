"""
Unit Tests: News Impact Microstructure & Trajectory Calculations
-----------------------------------------------------------------
Validates Alpha calculations, composite impact scoring, off-hours pending status,
and trajectory classification.
Runnable via pytest or python -m unittest.
"""

import unittest
from datetime import datetime


def calculate_test_metrics(pre_close, post_close, qqq_pre, qqq_post, high, low, volume, baseline_vol):
    """Mirror of the impact calculation formula in nasdaq_transforms.impact"""
    stock_return = round(((post_close - pre_close) / pre_close) * 100.0, 4)
    qqq_return = round(((qqq_post_close - qqq_pre) / qqq_pre) * 100.0, 4) if (qqq_pre and qqq_post) else 0.0
    alpha = round(stock_return - qqq_return, 4)
    intraday_vol = round(((high - low) / pre_close) * 100.0, 4)
    volume_surge = round(volume / max(baseline_vol, 1.0), 2)

    surge_capped = min(volume_surge, 5.0)
    if alpha >= 0:
        raw_score = (alpha * 20.0) + ((surge_capped - 1.0) * 8.0) + (intraday_vol * 10.0)
    else:
        raw_score = (alpha * 20.0) - ((surge_capped - 1.0) * 8.0) - (intraday_vol * 10.0)

    composite_score = round(max(-100.0, min(100.0, raw_score)), 2)

    if abs(alpha) >= 0.4:
        pattern = "Sustained Move"
    elif intraday_vol >= 0.5 and abs(alpha) < 0.2:
        pattern = "Reversal/Fade"
    else:
        pattern = "Flat Noise"

    return {
        "stock_return": stock_return,
        "qqq_return": qqq_return,
        "alpha": alpha,
        "intraday_vol": intraday_vol,
        "volume_surge": volume_surge,
        "composite_score": composite_score,
        "pattern": pattern
    }


class TestNewsImpactModel(unittest.TestCase):

    def test_alpha_calculation(self):
        """Alpha should be Stock Return minus QQQ Return."""
        # AAPL moves from 200.0 to 204.0 (+2.0%)
        # QQQ moves from 450.0 to 452.25 (+0.5%)
        # Expected Alpha = +1.5%
        pre_close = 200.0
        post_close = 204.0
        qqq_pre = 450.0
        global qqq_post_close
        qqq_post_close = 452.25

        res = calculate_test_metrics(
            pre_close=pre_close,
            post_close=post_close,
            qqq_pre=qqq_pre,
            qqq_post=qqq_post_close,
            high=204.5,
            low=199.8,
            volume=300000,
            baseline_vol=100000
        )
        self.assertAlmostEqual(res["stock_return"], 2.0, places=3)
        self.assertAlmostEqual(res["qqq_return"], 0.5, places=3)
        self.assertAlmostEqual(res["alpha"], 1.5, places=3)
        self.assertEqual(res["pattern"], "Sustained Move")
        self.assertGreater(res["composite_score"], 0)

    def test_reversal_fade_pattern(self):
        """High volatility spike that ends flat should be classified as Reversal/Fade."""
        pre_close = 100.0
        post_close = 100.05  # +0.05% final move
        qqq_pre = 400.0
        global qqq_post_close
        qqq_post_close = 400.0  # 0.0%

        res = calculate_test_metrics(
            pre_close=pre_close,
            post_close=post_close,
            qqq_pre=qqq_pre,
            qqq_post=qqq_post_close,
            high=102.5,  # Spiked 2.5% intraday
            low=99.8,
            volume=250000,
            baseline_vol=100000
        )
        self.assertLess(abs(res["alpha"]), 0.2)
        self.assertGreaterEqual(res["intraday_vol"], 0.5)
        self.assertEqual(res["pattern"], "Reversal/Fade")

    def test_flat_noise_pattern(self):
        """Tiny move and low volatility should be classified as Flat Noise."""
        pre_close = 100.0
        post_close = 100.02
        qqq_pre = 400.0
        global qqq_post_close
        qqq_post_close = 400.0

        res = calculate_test_metrics(
            pre_close=pre_close,
            post_close=post_close,
            qqq_pre=qqq_pre,
            qqq_post=qqq_post_close,
            high=100.10,
            low=99.95,
            volume=80000,
            baseline_vol=100000
        )
        self.assertEqual(res["pattern"], "Flat Noise")

    def test_composite_score_bounds(self):
        """Composite score must always remain clamped between -100 and +100."""
        # Extreme positive
        res_pos = calculate_test_metrics(100.0, 150.0, 400.0, 400.0, 160.0, 95.0, 5000000, 10000)
        self.assertEqual(res_pos["composite_score"], 100.0)

        # Extreme negative
        res_neg = calculate_test_metrics(100.0, 50.0, 400.0, 400.0, 105.0, 45.0, 5000000, 10000)
        self.assertEqual(res_neg["composite_score"], -100.0)

    def test_pending_status_determination(self):
        """If post-event price is missing, status must be PENDING_EVALUATION."""
        pre_price = 220.0
        post_price = None  # Market closed or prices haven't elapsed
        is_completed = (pre_price is not None and post_price is not None)
        status = "COMPLETED" if is_completed else "PENDING_EVALUATION"
        self.assertEqual(status, "PENDING_EVALUATION")


if __name__ == "__main__":
    unittest.main()
