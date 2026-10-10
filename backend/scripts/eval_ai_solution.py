"""Evaluation script for the AI solution block & Decision LLM.

Runs a curated benchmark of realistic failure logs with ground-truth fixes,
and computes Precision, Recall, F1-Score (macro, weighted, and per-class),
and overall Accuracy.

Usage:
    cd backend
    python scripts/eval_ai_solution.py
    python scripts/eval_ai_solution.py --live   # calls actual Ollama / LLM endpoint
"""

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from unittest.mock import MagicMock

# Allow imports from app
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.automation.remediation import Advice, Choice, Facts, Settings as RemediationSettings, choose_fix
from app.connectors.decision_llm import get_decision_llm
from app.core.config import get_settings
from app.diagnosis.log_classifier import FailureCategory


# ----------------------------------------------------------------------
# Benchmark Dataset with Ground-Truth Annotations
# ----------------------------------------------------------------------
BENCHMARK_CASES = [
    {
        "id": "transient_timeout_1",
        "dag_id": "daily_reporting",
        "category": FailureCategory.TIMEOUT.value,
        "log": """
        [2026-04-01 02:15:33] {base.py:73} ERROR - airflow.exceptions.AirflowSensorTimeout: Sensor has timed out after 3600 seconds.
        [2026-04-01 02:15:33] {taskinstance.py:1937} ERROR - Task failed with exception
        """,
        "fix_attempts": 0,
        "failed_verifications": 0,
        "ground_truth_fix": "retry",
    },
    {
        "id": "transient_network_conn_reset",
        "dag_id": "payment_sync",
        "category": FailureCategory.TRANSIENT_NETWORK.value,
        "log": """
        [2026-04-01 04:10:05] {requests.py:120} ERROR - requests.exceptions.ConnectionError: ('Connection aborted.', ConnectionResetError(10054, 'An existing connection was forcibly closed by the remote host'))
        [2026-04-01 04:10:05] {taskinstance.py:1937} ERROR - Task failed.
        """,
        "fix_attempts": 0,
        "failed_verifications": 0,
        "ground_truth_fix": "retry",
    },
    {
        "id": "oom_resource_kill",
        "dag_id": "ml_training_pipeline",
        "category": FailureCategory.RESOURCE.value,
        "log": """
        [2026-04-01 06:30:19] {spark_submit.py:45} ERROR - java.lang.OutOfMemoryError: Java heap space
        Container killed by YARN for exceeding memory limits. 16.4 GB of 16 GB physical memory used.
        """,
        "fix_attempts": 0,
        "failed_verifications": 0,
        "ground_truth_fix": "retry",
    },
    {
        "id": "sql_syntax_code_bug",
        "dag_id": "customer_360",
        "category": FailureCategory.CODE_BUG.value,
        "log": """
        [2026-04-01 08:00:22] {postgres.py:90} ERROR - psycopg2.errors.SyntaxError: syntax error at or near "SELCT"
        LINE 1: SELCT user_id, email FROM users
                ^
        """,
        "fix_attempts": 0,
        "failed_verifications": 0,
        "ground_truth_fix": "escalate",
    },
    {
        "id": "schema_column_missing",
        "dag_id": "warehouse_ingest",
        "category": FailureCategory.SCHEMA.value,
        "log": """
        [2026-04-01 09:12:44] {snowflake.py:110} ERROR - snowflake.connector.errors.ProgrammingError: 000904 (42000): SQL compilation error:
        error: column 'TRANSACTION_TS' does not exist in target table 'STAGE_TRANSACTIONS'
        """,
        "fix_attempts": 0,
        "failed_verifications": 0,
        "ground_truth_fix": "escalate",
    },
    {
        "id": "auth_token_expired",
        "dag_id": "salesforce_export",
        "category": FailureCategory.AUTH.value,
        "log": """
        [2026-04-01 10:20:00] {oauth.py:52} ERROR - oauthlib.oauth2.rfc6749.errors.InvalidGrantError: (invalid_grant) expired access/refresh token or revoked credentials
        HTTP 401 Unauthorized
        """,
        "fix_attempts": 0,
        "failed_verifications": 0,
        "ground_truth_fix": "escalate",
    },
    {
        "id": "repeated_failed_retry_exhausted",
        "dag_id": "daily_reporting",
        "category": FailureCategory.TIMEOUT.value,
        "log": """
        [2026-04-01 11:45:00] {taskinstance.py:1937} ERROR - Task failed again after multiple automatic attempts.
        Timed out waiting for upstream dependency.
        """,
        "fix_attempts": 2,
        "failed_verifications": 2,
        "ground_truth_fix": "escalate",
    },
    {
        "id": "file_not_arrived_yet",
        "dag_id": "partner_file_import",
        "category": FailureCategory.UPSTREAM_MISSING.value,
        "log": """
        [2026-04-01 12:00:15] {s3_sensor.py:88} WARNING - S3 key 'partner_uploads/2026-04-01/dump.csv' does not exist yet.
        Waiting for file to land.
        """,
        "fix_attempts": 0,
        "failed_verifications": 0,
        "ground_truth_fix": "wait",
    },
]


# ----------------------------------------------------------------------
# Metrics calculation (Precision, Recall, F1)
# ----------------------------------------------------------------------
@dataclass
class ClassificationMetrics:
    precision: float
    recall: float
    f1: float
    support: int


def calculate_metrics(y_true: list[str], y_pred: list[str]) -> dict:
    classes = sorted(list(set(y_true) | set(y_pred)))
    per_class: dict[str, ClassificationMetrics] = {}
    total_samples = len(y_true)

    tp_count = defaultdict(int)
    fp_count = defaultdict(int)
    fn_count = defaultdict(int)
    support_count = defaultdict(int)

    for yt, yp in zip(y_true, y_pred):
        support_count[yt] += 1
        if yt == yp:
            tp_count[yt] += 1
        else:
            fp_count[yp] += 1
            fn_count[yt] += 1

    macro_p, macro_r, macro_f1 = 0.0, 0.0, 0.0
    weighted_p, weighted_r, weighted_f1 = 0.0, 0.0, 0.0

    for cls in classes:
        tp = tp_count[cls]
        fp = fp_count[cls]
        fn = fn_count[cls]
        sup = support_count[cls]

        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) > 0 else 0.0

        per_class[cls] = ClassificationMetrics(precision=prec, recall=rec, f1=f1, support=sup)
        macro_p += prec
        macro_r += rec
        macro_f1 += f1
        weighted_p += prec * sup
        weighted_r += rec * sup
        weighted_f1 += f1 * sup

    n_classes = len(classes) or 1
    accuracy = sum(1 for yt, yp in zip(y_true, y_pred) if yt == yp) / total_samples if total_samples > 0 else 0.0

    return {
        "accuracy": accuracy,
        "macro_precision": macro_p / n_classes,
        "macro_recall": macro_r / n_classes,
        "macro_f1": macro_f1 / n_classes,
        "weighted_precision": weighted_p / total_samples if total_samples > 0 else 0.0,
        "weighted_recall": weighted_r / total_samples if total_samples > 0 else 0.0,
        "weighted_f1": weighted_f1 / total_samples if total_samples > 0 else 0.0,
        "per_class": per_class,
    }


def evaluate(live: bool = False):
    settings = get_settings()
    llm = get_decision_llm() if live else None

    print("\n=======================================================")
    print("   AI Solution Block - Performance Evaluation")
    print("=======================================================")
    print(f"Mode: {'LIVE LLM (Ollama @ ' + settings.DECISION_LLM_URL + ')' if live else 'DETERMINISTIC / RULE ENGINE BENCHMARK'}")
    print(f"Total Test Cases: {len(BENCHMARK_CASES)}\n")

    y_true = []
    y_pred = []
    details = []

    for item in BENCHMARK_CASES:
        facts = Facts(
            incident_type="DAG_RUN_FAILED",
            incident_status="OPEN",
            dag_id=item["dag_id"],
            environment="production",
            severity="HIGH",
            occurrences=1,
            category=item["category"],
            confidence=0.9,
            source="regex",
            has_failed_run=True,
            fix_attempts=item["fix_attempts"],
            failed_verifications=item["failed_verifications"],
            actions_today=1,
            max_actions_per_day=3,
            dag_state="ready",
        )
        choice = choose_fix(facts, RemediationSettings())
        logs = [item["log"]]
        diagnosis = {"label": item["category"], "category": item["category"]}

        if live and llm and llm.enabled:
            advice = llm.advise(facts, choice, diagnosis, logs)
            pred = advice.fix
            reason = advice.reason
            conf = advice.confidence
        else:
            # Baseline remediation decision logic (guardrail engine)
            pred = choice.fix
            reason = choice.reason
            conf = 0.95

        y_true.append(item["ground_truth_fix"])
        y_pred.append(pred)
        match_str = "[PASS]" if pred == item["ground_truth_fix"] else "[FAIL]"
        details.append({
            "id": item["id"],
            "true": item["ground_truth_fix"],
            "pred": pred,
            "status": match_str,
            "reason": reason,
            "confidence": conf,
        })

    # Print itemized results
    print(f"{'Case ID':<32} {'Expected':<10} {'Predicted':<10} {'Status':<8} {'Confidence':<10}")
    print("-" * 75)
    for d in details:
        print(f"{d['id']:<32} {d['true']:<10} {d['pred']:<10} {d['status']:<8} {d['confidence']*100:.0f}%")

    metrics = calculate_metrics(y_true, y_pred)

    print("\n-------------------------------------------------------")
    print("   Per-Class Evaluation (F1-Score, Precision, Recall)")
    print("-------------------------------------------------------")
    print(f"{'Class':<14} {'Precision':<12} {'Recall':<12} {'F1-Score':<12} {'Support':<8}")
    print("-" * 58)
    for cls, m in metrics["per_class"].items():
        print(f"{cls:<14} {m.precision:<12.2f} {m.recall:<12.2f} {m.f1:<12.2f} {m.support:<8}")

    print("\n-------------------------------------------------------")
    print("   Summary Metrics")
    print("-------------------------------------------------------")
    print(f"Overall Accuracy:   {metrics['accuracy'] * 100:.1f}%")
    print(f"Macro F1-Score:     {metrics['macro_f1']:.3f}   (Precision: {metrics['macro_precision']:.3f}, Recall: {metrics['macro_recall']:.3f})")
    print(f"Weighted F1-Score:  {metrics['weighted_f1']:.3f}   (Precision: {metrics['weighted_precision']:.3f}, Recall: {metrics['weighted_recall']:.3f})")
    print("=======================================================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate AI solution performance with F1 score")
    parser.add_argument("--live", action="store_true", help="Run against the live Ollama decision LLM")
    args = parser.parse_args()
    evaluate(live=args.live)
