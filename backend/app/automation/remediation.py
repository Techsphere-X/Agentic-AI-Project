"""Choose the fix for an incident: a deterministic, explainable decision table.

Pure: no database, HTTP, or framework imports. The "Choose the fix" block gathers the facts
(diagnosis, fix history, action budget, DAG state) and this module turns them into one fix plus
the set of fixes that are safe at all. The AI assistant may only pick from that set, so the rules
here are the guard rail for both.
"""

from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal

from app.diagnosis.log_classifier import FailureCategory

FIXES: tuple[str, ...] = ("retry", "rerun", "wait", "pause", "escalate", "ignore")
FIX_LABELS: dict[str, str] = {
    "retry": "retry the failed tasks",
    "rerun": "start a new run",
    "wait": "wait for the input",
    "pause": "pause the DAG",
    "escalate": "hand it to a person",
    "ignore": "leave it alone",
}

_C = FailureCategory
# Retrying cannot help: the data, the schema, the code or the credentials have to change first.
NEEDS_FIX = frozenset({_C.DATA_INTEGRITY, _C.SCHEMA, _C.CODE_BUG, _C.AUTH})
BAD_DATA = frozenset({_C.DATA_INTEGRITY, _C.SCHEMA})
RETRY_HELPS = frozenset({_C.TRANSIENT_NETWORK, _C.TIMEOUT, _C.RESOURCE})


@dataclass(frozen=True)
class Facts:
    incident_type: str  # DAG_RUN_FAILED / SLA_MISSED
    incident_status: str  # OPEN / ACKNOWLEDGED / RESOLVED
    dag_id: str
    environment: str
    severity: str
    occurrences: int
    category: str  # FailureCategory value
    confidence: float
    source: str  # regex / model / operator
    has_failed_run: bool  # there is a failed run whose tasks can be cleared
    fix_attempts: int  # earlier automated retries / reruns on this incident that were carried out
    failed_verifications: int  # of those, how many did not make the run succeed
    actions_today: int  # automated actions on this DAG in the last 24h (policy rate limit)
    max_actions_per_day: int
    dag_state: str  # ready / paused / busy / unknown

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Settings:
    max_attempts: int = 2
    min_confidence_pct: int | None = None
    retry_unknown_once: bool = True
    pause_on_bad_data: bool = False
    pause_after_failures: int | None = None


@dataclass(frozen=True)
class Choice:
    fix: str
    rule: str
    reason: str
    allowed: tuple[str, ...]  # every fix the rules consider safe here (fix is one of them)

    def as_dict(self) -> dict[str, object]:
        return {**asdict(self), "allowed": list(self.allowed)}


def _only(fix: str, rule: str, reason: str) -> Choice:
    return Choice(fix, rule, reason, (fix,))


def _stop(fix: str, rule: str, reason: str, pause_ok: bool) -> Choice:
    """No retry or rerun is safe; a person (or pausing, where enabled) is."""
    return Choice(fix, rule, reason, ("escalate", "pause") if pause_ok else ("escalate",))


def choose_fix(facts: Facts, settings: Settings) -> Choice:
    """First matching rule wins; see FIX_LABELS for what each fix means."""
    dag = facts.dag_id
    # States where only one answer makes sense: the AI is not asked about these.
    if facts.incident_status == "RESOLVED":
        return _only("ignore", "already_resolved", "The incident is already resolved.")
    if facts.dag_state == "paused":
        return _only("ignore", "dag_paused", f"{dag} is paused in Airflow; someone stopped it.")
    choice = _choose(facts, settings)
    if facts.dag_state != "busy" or "rerun" not in choice.allowed:
        return choice
    # A run in progress only rules out starting another one; retrying failed tasks is fine.
    allowed = tuple(f for f in choice.allowed if f != "rerun")
    if choice.fix == "rerun":
        return Choice(
            "wait",
            "run_active",
            f"{dag} already has a run in progress; wait for it instead of starting another.",
            allowed,
        )
    return replace(choice, allowed=allowed)


def _choose(facts: Facts, settings: Settings) -> Choice:
    dag = facts.dag_id
    category = _category(facts, settings)
    attempts = facts.fix_attempts
    tried = f"{attempts} automatic fix{'es' if attempts != 1 else ''} already tried"
    if facts.failed_verifications:
        tried += f", {facts.failed_verifications} did not work"
    may_pause = settings.pause_on_bad_data or settings.pause_after_failures is not None

    if category in NEEDS_FIX:
        what = f"{facts.category.replace('_', ' ').lower()} failures need a change, not a retry"
        if settings.pause_on_bad_data and category in BAD_DATA:
            return _stop(
                "pause",
                "stop_bad_data",
                f"Bad data: pausing {dag} so it spreads no further ({what}).",
                True,
            )
        return _stop("escalate", "needs_fix", f"Retrying will not help: {what}.", may_pause)
    if (
        settings.pause_after_failures is not None
        and facts.occurrences >= settings.pause_after_failures
    ):
        return _stop(
            "pause", "keeps_failing", f"{dag} failed {facts.occurrences} times; pausing it.", True
        )
    if attempts >= settings.max_attempts:
        return _stop(
            "escalate", "out_of_attempts", f"{tried} (limit {settings.max_attempts}).", may_pause
        )
    if facts.actions_today >= facts.max_actions_per_day:
        return _stop(
            "escalate",
            "daily_limit",
            f"{facts.actions_today} automated actions on "
            f"{dag} in the last 24h (limit {facts.max_actions_per_day}).",
            may_pause,
        )

    # A retry or rerun is acceptable from here on.
    actions = ("retry", "rerun") if facts.has_failed_run else ("rerun",)
    allowed = (*actions, "wait", "escalate", *(("pause",) if may_pause else ()))
    if facts.incident_type == "SLA_MISSED":
        return Choice(
            "rerun",
            "sla_missed",
            f"{dag} missed its SLA and nothing is running; start a new run.",
            allowed,
        )
    if category == _C.UPSTREAM_MISSING:
        return Choice(
            "wait",
            "missing_input",
            "The input is not there yet; wait for it, then run again.",
            allowed,
        )
    fix = actions[0]
    if category in RETRY_HELPS:
        label = facts.category.replace("_", " ").lower()
        return Choice(
            fix,
            "retry_helps",
            f"A {label} failure usually passes; "
            f"{FIX_LABELS[fix]}{f' ({tried})' if attempts else ''}.",
            allowed,
        )
    # UNKNOWN (or a diagnosis below the confidence minimum).
    if settings.retry_unknown_once and attempts == 0:
        return Choice(
            fix,
            "unknown_try_once",
            "The cause is unclear; one automatic "
            f"attempt to {FIX_LABELS[fix]} before asking a person.",
            allowed,
        )
    return Choice("escalate", "unknown", "The cause is unclear and a person should look.", allowed)


def _category(facts: Facts, settings: Settings) -> FailureCategory:
    try:
        category = FailureCategory(facts.category)
    except ValueError:
        return _C.UNKNOWN
    if (
        settings.min_confidence_pct is not None
        and facts.source != "operator"
        and facts.confidence * 100 < settings.min_confidence_pct
    ):
        return _C.UNKNOWN
    return category


# ---------------------------------------------------------------------- AI advice

AIMode = Literal["off", "suggest", "decide"]


class AdvisorUnavailable(Exception):
    """The model could not be asked or gave no usable answer (down, slow, bad JSON, breaker)."""


@dataclass(frozen=True)
class Advice:
    fix: str
    reason: str
    confidence: float
    model: str | None = None


@dataclass(frozen=True)
class Advised:
    fix: str
    reason: str
    ai: dict[str, Any]  # what the model said and whether it routed; stored with the decision


def advise(
    choice: Choice,
    mode: AIMode,
    ask: Callable[[], Advice] | None,
    min_confidence_pct: int | None = None,
) -> Advised:
    """Let the model pick among `choice.allowed`; the rules' choice stands whenever it can't.

    `ask` is None when the model is disabled. In "suggest" mode the model's answer is only
    recorded; in "decide" mode a usable answer routes the block.
    """
    ai: dict[str, Any] = {"mode": mode, "status": "off", "routed": False}
    if mode == "off":
        return Advised(choice.fix, choice.reason, ai)
    if len(choice.allowed) <= 1:
        return Advised(choice.fix, choice.reason, {**ai, "status": "skipped"})
    if ask is None:
        return Advised(choice.fix, choice.reason, {**ai, "status": "disabled"})
    try:
        advice = ask()
    except AdvisorUnavailable as exc:
        return Advised(
            choice.fix, choice.reason, {**ai, "status": "unavailable", "error": str(exc)}
        )
    ai.update(
        fix=advice.fix,
        reason=advice.reason,
        confidence=round(advice.confidence, 4),
        model=advice.model,
    )
    if advice.fix not in choice.allowed:
        ai["status"] = "rejected"
    elif min_confidence_pct is not None and advice.confidence * 100 < min_confidence_pct:
        ai["status"] = "low_confidence"
    else:
        ai["status"] = "used"
    if ai["status"] != "used" or mode != "decide":
        return Advised(choice.fix, choice.reason, ai)
    ai["routed"] = True
    return Advised(advice.fix, f"AI ({advice.model or 'local model'}): {advice.reason}", ai)
