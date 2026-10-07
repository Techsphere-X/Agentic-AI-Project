"""Deterministic failure classification from task log text.

Pure: no database, HTTP, or framework imports. Rules are checked in order and the first match
wins, so specific causes (bad data, auth) are listed before generic ones (code errors).

Rules only see the log's *signal* lines: INFO/DEBUG/WARNING lines, traceback frames and their
source lines are dropped first, so a recovered retry, a token refresh or a `raise ConnectionError`
frame elsewhere in the stack cannot decide the category. A killed process is the one cause Airflow
reports at INFO level ("Task exited with return code -9"), so that rule alone sees the full log.
"""

import re
from dataclasses import asdict, dataclass
from enum import StrEnum


class FailureCategory(StrEnum):
    AUTH = "AUTH"
    DATA_INTEGRITY = "DATA_INTEGRITY"
    SCHEMA = "SCHEMA"
    CODE_BUG = "CODE_BUG"
    RESOURCE = "RESOURCE"
    TIMEOUT = "TIMEOUT"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    UPSTREAM_MISSING = "UPSTREAM_MISSING"
    UNKNOWN = "UNKNOWN"


CATEGORY_LABELS: dict[FailureCategory, str] = {
    FailureCategory.AUTH: "Login or permission problem",
    FailureCategory.DATA_INTEGRITY: "Bad data (constraint violation)",
    FailureCategory.SCHEMA: "Table or column changed",
    FailureCategory.CODE_BUG: "Code error",
    FailureCategory.RESOURCE: "Out of memory or disk",
    FailureCategory.TIMEOUT: "Timeout",
    FailureCategory.TRANSIENT_NETWORK: "Network glitch",
    FailureCategory.UPSTREAM_MISSING: "Missing input",
    FailureCategory.UNKNOWN: "Unknown",
}

RETRYABLE: frozenset[FailureCategory] = frozenset(
    {
        FailureCategory.RESOURCE,
        FailureCategory.TIMEOUT,
        FailureCategory.TRANSIENT_NETWORK,
        FailureCategory.UPSTREAM_MISSING,
        FailureCategory.UNKNOWN,
    }
)


@dataclass(frozen=True)
class _Rule:
    name: str
    category: FailureCategory
    pattern: re.Pattern[str]
    confidence: float


def _rule(name: str, category: FailureCategory, regex: str, confidence: float = 0.9) -> _Rule:
    return _Rule(name, category, re.compile(regex, re.IGNORECASE), confidence)


def _status(*codes: str) -> str:
    """An HTTP status code as a standalone number, never part of a timestamp or a longer number."""
    return rf"(?<![\d.:])(?:{'|'.join(codes)})(?![\d.:])"


_C = FailureCategory
_KILLED = r"OOMKilled|exit code (?:-9|137)|return code -9|\bSIGKILL\b"
RULES: tuple[_Rule, ...] = (
    _rule(
        "out_of_resources",
        _C.RESOURCE,
        rf"MemoryError|out of memory|No space left on device|{_KILLED}|"
        r"memory limits?\b.*exceed|exceed\w* (?:its |the )?memory limits?|Java heap space|"
        r"GC overhead limit|Cannot allocate memory|Disk quota exceeded|"
        r"\bquota\b.*exceeded|exceeded (?:its |your )?quota|Too many open files|"
        r"\bevicted\b|low on resource|pool exhausted|ResourceExhausted|LimitExceeded|"
        r"limit of concurrent|ExecutorLostFailure|too many clients|TooManyConnections|"
        r"bigger than spark\.driver\.maxResultSize|Insufficient (?:memory|cpu)|killed by signal|"
        r"exceeded \S+ memory budget|remaining connection slots are reserved",
    ),
    _rule(
        "auth_failure",
        _C.AUTH,
        r"authentication failed|AuthenticationFailed|ClientAuthenticationError|"
        r"invalid credentials|bad credentials|incorrect (?:username|password)|"
        r"permission denied|Permission '[^']*' denied|access ?denied|unauthori[sz]ed|"
        rf"(?:status|HTTP|code)\W{{0,3}}(?:{_status('401', '403')})|"
        rf"(?:{_status('403')}).*forbidden|forbidden.*(?:{_status('403')})|"
        r"(?:token|credentials?|secret(?: keys?)?)(?: \S+)?(?: has| have| were| are)?"
        r" (?:been )?(?:expired|revoked)|invalid_grant|invalid_auth|invalid api key|"
        r"InvalidClientTokenId|InvalidAccessKeyId|SignatureDoesNotMatch|ExpiredToken|"
        r"InvalidSignature|signature verification failed|AADSTS\d+|login (?:rejected|failed)|"
        r"missing client token|DefaultCredentialsError|NoCredentialsError|"
        r"credentials were not found|no pg_hba\.conf entry|password authentication failed|"
        r"UnrecognizedClientException|security token included in the request is invalid",
    ),
    _rule(
        "constraint_violation",
        _C.DATA_INTEGRITY,
        r"UniqueViolation|duplicate key|violates (?:unique|foreign key|not-null|check) constraint|"
        r"IntegrityError|ForeignKeyViolation|NotNullViolation|CheckViolation",
        0.95,
    ),
    _rule(
        "schema_change",
        _C.SCHEMA,
        r"UndefinedColumn|UndefinedTable|column \S+ (?:of relation \S+ )?does not exist|"
        r"relation \S+ does not exist|no such (?:table|column)|schema mismatch|"
        r"invalid identifier|Unknown column|Invalid column name|"
        r"Table '[^']+' doesn't exist|UNRESOLVED_COLUMN|cannot be resolved|"
        r"Referenced column \S+ not found|Unrecognized name|not in index|"
        r"Schema (?:at index \d+ was different|does not match)|has changed type|DatatypeMismatch|"
        r"is of type .* but expression is of type|Length mismatch|Columns must be same length|"
        r"declared by the target table schema|upstream schema|schema (?:drift|change)|"
        r"field \S+ (?:was )?removed|removed from .*(?:response|schema|API)|NoSuchTableError|"
        r"TABLE_OR_VIEW_NOT_FOUND|does not exist or not authorized|CANNOT_MERGE_SCHEMAS|"
        r"SchemaResolutionError|Column\(s\) .* do not exist|columns expected but not found",
    ),
    _rule(
        "bad_data",
        _C.DATA_INTEGRITY,
        r"data quality check failed|quality check failed|duplicate (?:rows|records|entries)|"
        r"Error tokenizing data|Expected \d+ fields|Numeric value .* is not recognized|"
        r"InvalidTextRepresentation|invalid input syntax for|StringDataRightTruncation|"
        r"value too long for type|NumericValueOutOfRange|numeric field overflow|"
        r"encountered too many errors|Failure in test|configured to fail|"
        r"SQLCheckOperator: Test failed|UnicodeDecodeError|codec can't decode|"
        r"does not match format|Validation failed|unexpected values|row count mismatch|"
        r"Could not convert|\bDataError\b|Incorrect (?:integer|decimal|date|datetime) value|"
        r"rows? with NULL|invalid literal for int\(\)|Could not parse \S+ as|"
        r"\b(?:Date|Timestamp|Numeric value) \S+ is not recognized|unconverted data remains",
        0.9,
    ),
    _rule(
        "timeout",
        _C.TIMEOUT,
        r"AirflowTaskTimeout|AirflowSensorTimeout|TimeoutError|timed out|deadline exceeded|"
        r"Deadline of \S+ exceeded|ReadTimeout|ConnectTimeout|statement timeout|"
        r"execution timeout|(?:warehouse|query|statement) timeout|"
        r"execution time exceeded|did not (?:finish|complete) within|"
        r"has not completed after|Max attempts exceeded|exceeded max wait|"
        r"still \w+ after \d+|gave up after|lock timeout|STATEMENT_TIMEOUT",
    ),
    _rule(
        "network_error",
        _C.TRANSIENT_NETWORK,
        r"ConnectionError|Connection (?:refused|reset|aborted)|ConnectionResetError|"
        r"Temporary failure in name resolution|Name or service not known|"
        rf"{_status('502', '503', '504', '429')}|Bad Gateway|Service Unavailable|"
        r"Gateway Time-?out|Too Many Requests|could not connect to server|"
        r"server closed the connection|BrokenPipeError|SSL: UNEXPECTED_EOF|SSLError|"
        r"SSL SYSCALL error|RemoteDisconnected|Server disconnected|RemoteProtocolError|"
        r"StatusCode\.UNAVAILABLE|failed to connect to all addresses|Lost connection to|"
        r"NoBrokersAvailable|upstream connect error|reset before headers|"
        r"Failed to get the response|peer closed connection|unexpected EOF|"
        r"Network is unreachable|No route to host|ECONNRESET|ECONNREFUSED",
    ),
    _rule(
        "missing_input",
        _C.UPSTREAM_MISSING,
        r"FileNotFoundError|NoSuchKey|No such file or directory|NoSuchBucket|No such object|"
        r"partition \S+ not found|upstream \S* ?not ready|path does not exist|"
        r"object does not exist|\bNotFound: 404|\(404\)|was not found|No objects found|"
        r"returned no objects|bucket is empty|not yet available|is not ready|"
        r"EmptyDataError|No columns to parse|external task \S+ in DAG \S+ failed|"
        r"has no new version|was never written|expected \d+ .*partitions?, found \d+",
    ),
    _rule(
        "code_error",
        _C.CODE_BUG,
        r"SyntaxError|IndentationError|NameError|UnboundLocalError|ImportError|"
        r"ModuleNotFoundError|AttributeError|TypeError|KeyError|IndexError|ZeroDivisionError|"
        r"ValueError|AssertionError|NotImplementedError|RecursionError|UndefinedError|"
        r"command not found|Invalid arguments were passed|XComArg result .* not found|"
        r"failed to parse|Broken DAG",
        0.7,
    ),
)
# Airflow logs a killed task at INFO level, so this one rule also searches the full log.
_FULL_LOG_RULE = _rule("process_killed", _C.RESOURCE, _KILLED)

# Lines that never carry the cause: low log levels, traceback headers, frames and carets.
_NOISE_LEVEL = re.compile(r"^\s*(?:\[[^\]]*\]\s*)?(?:\{[^}]*\}\s*)?(?:INFO|DEBUG|WARNING|WARN)\b")
_FRAME = re.compile(r'^\s*File "[^"]*", line \d+')
_CARETS = re.compile(r"^\s*[\^~]+\s*$")
_ANSI = re.compile(r"\x1b\[[0-9;]*m")


@dataclass(frozen=True)
class Diagnosis:
    category: FailureCategory
    label: str
    retryable: bool
    confidence: float
    rule: str | None = None
    matched_line: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _unknown(reason: str) -> Diagnosis:
    return Diagnosis(
        category=FailureCategory.UNKNOWN,
        label=CATEGORY_LABELS[FailureCategory.UNKNOWN],
        retryable=True,
        confidence=0.0,
        rule=reason,
    )


def _line_at(text: str, index: int) -> str:
    start = text.rfind("\n", 0, index) + 1
    end = text.find("\n", index)
    line = text[start : end if end != -1 else len(text)].strip()
    return line[:300]


def _signal_text(log_text: str) -> str:
    """The log without lines that never carry the cause (see the module docstring)."""
    kept: list[str] = []
    skip_source = False
    for line in _ANSI.sub("", log_text).splitlines():
        if skip_source:  # the source line printed under a traceback frame
            skip_source = False
            if not _FRAME.match(line):
                continue
        if _FRAME.match(line):
            skip_source = True
            continue
        if _NOISE_LEVEL.match(line) or _CARETS.match(line) or line.startswith("Traceback ("):
            continue
        kept.append(line)
    return "\n".join(kept)


def _diagnosis(rule: _Rule, text: str, index: int) -> Diagnosis:
    return Diagnosis(
        category=rule.category,
        label=CATEGORY_LABELS[rule.category],
        retryable=rule.category in RETRYABLE,
        confidence=rule.confidence,
        rule=rule.name,
        matched_line=_line_at(text, index),
    )


def classify(log_text: str | None) -> Diagnosis:
    """Classify a failure from its log text (first matching rule wins)."""
    if not log_text or not log_text.strip():
        return _unknown("no_log")
    signal = _signal_text(log_text)
    for rule in RULES:
        match = rule.pattern.search(signal)
        if match:
            return _diagnosis(rule, signal, match.start())
    match = _FULL_LOG_RULE.pattern.search(log_text)
    if match:
        return _diagnosis(_FULL_LOG_RULE, log_text, match.start())
    return _unknown("no_rule_matched")


def classify_logs(logs: list[str | None]) -> Diagnosis:
    """Classify the first log (newest first) that yields a known category."""
    fallback = _unknown("no_log")
    for text in logs:
        diagnosis = classify(text)
        if diagnosis.category != FailureCategory.UNKNOWN:
            return diagnosis
        if text:
            fallback = diagnosis
    return fallback
