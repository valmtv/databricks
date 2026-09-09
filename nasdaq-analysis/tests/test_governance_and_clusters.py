"""
Unit Tests: Story Cluster Classification & Governance Policies (RLS / CLS)
---------------------------------------------------------------------------
Validates:
1. Story cluster classification mapping rules (Apple, Power Grid, NVIDIA, LLMs, Earnings).
2. Row-Level Security (RLS) filtering: hiding pending news from unauthorized roles.
3. Column-Level Security (CLS) score masking for non-premium users.
"""

import unittest


def classify_story_cluster(headline: str) -> str:
    """Exact logic used in dashboard narrative clustering."""
    hl = (headline or "").lower()
    if any(k in hl for k in ["ceo", "ternus", "tim cook"]):
        return "Apple Leadership & CEO Succession"
    elif any(k in hl for k in ["power", "grid", "emergency", "bloom energy"]):
        return "AI Power Grid & Energy Bottlenecks"
    elif any(k in hl for k in ["whale", "iren", "debt deal", "blackwell"]):
        return "NVIDIA Hyperscaler & Whale Flows"
    elif any(k in hl for k in ["claude", "anthropic", "openai", "copilot"]):
        return "LLM & Foundation Model Wars"
    elif any(k in hl for k in ["earnings", "q2", "q3"]):
        return "Quarterly Financial Results & Guidance"
    else:
        return "Corporate & Industry Developments"


def evaluate_rls_filter(user: str, user_groups: list, impact_status: str) -> bool:
    """Mirrors fact_news_impact_rls_filter logic in 05_governance_rls_cls.sql."""
    allowed_users = ["valerii.matviiv@softserve.academy", "valerii.matviiv@gmail.com"]
    is_admin_or_senior = any(g in user_groups for g in ["admin_analysts", "senior_traders"]) or (user in allowed_users)
    if is_admin_or_senior:
        return True
    return impact_status == "COMPLETED"


def evaluate_cls_mask(user: str, user_groups: list, score: float):
    """Mirrors mask_composite_score logic in 05_governance_rls_cls.sql."""
    allowed_users = ["valerii.matviiv@softserve.academy", "valerii.matviiv@gmail.com"]
    is_authorized = any(g in user_groups for g in ["premium_traders", "admin_analysts"]) or (user in allowed_users)
    if is_authorized:
        return score
    return None  # Masked for standard analysts


class TestGovernanceAndClusters(unittest.TestCase):

    def test_story_cluster_categorization(self):
        """Validates keyword mapping into respective narrative investment clusters."""
        self.assertEqual(
            classify_story_cluster("Tim Cook discusses Apple succession plans"),
            "Apple Leadership & CEO Succession"
        )
        self.assertEqual(
            classify_story_cluster("Datacenter energy consumption triggers regional power grid alert"),
            "AI Power Grid & Energy Bottlenecks"
        )
        self.assertEqual(
            classify_story_cluster("NVIDIA Blackwell chip order backed by billion dollar debt deal"),
            "NVIDIA Hyperscaler & Whale Flows"
        )
        self.assertEqual(
            classify_story_cluster("OpenAI and Anthropic reveal new frontier models"),
            "LLM & Foundation Model Wars"
        )
        self.assertEqual(
            classify_story_cluster("Amazon beats Q2 revenue expectations with cloud surge"),
            "Quarterly Financial Results & Guidance"
        )
        self.assertEqual(
            classify_story_cluster("Tesla expands retail presence in suburban markets"),
            "Corporate & Industry Developments"
        )

    def test_rls_pending_news_protection(self):
        """Standard analysts must NOT see PENDING_EVALUATION news; Senior/Admins can see it."""
        # Standard user
        self.assertTrue(evaluate_rls_filter("analyst_bob", ["standard_analysts"], "COMPLETED"))
        self.assertFalse(evaluate_rls_filter("analyst_bob", ["standard_analysts"], "PENDING_EVALUATION"))

        # Senior trader
        self.assertTrue(evaluate_rls_filter("trader_alice", ["senior_traders"], "PENDING_EVALUATION"))

        # Admin user
        self.assertTrue(evaluate_rls_filter("valerii.matviiv@softserve.academy", [], "PENDING_EVALUATION"))

    def test_cls_composite_score_masking(self):
        """Standard analysts receive None for proprietary score; Premium traders see the score."""
        raw_score = 42.5

        # Standard user -> Masked to None
        self.assertIsNone(evaluate_cls_mask("analyst_bob", ["standard_analysts"], raw_score))

        # Premium user -> Sees raw score
        self.assertEqual(evaluate_cls_mask("trader_pro", ["premium_traders"], raw_score), raw_score)

        # Admin user -> Sees raw score
        self.assertEqual(evaluate_cls_mask("valerii.matviiv@softserve.academy", [], raw_score), raw_score)


if __name__ == "__main__":
    unittest.main()
