"""Cron schedules for "On a schedule": standard 5-field expressions, in a time zone.

    minute hour day-of-month month day-of-week

Fields take `*`, numbers, ranges (`1-5`), steps (`*/15`, `0-30/10`), lists (`1,15`) and names
(`jan`-`dec`, `sun`-`sat`). Day-of-week 0 and 7 are Sunday. As in cron, when both day fields
are restricted a day matches either. `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly`
are shorthands. Pure: no database access.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_MACROS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}
_MONTHS = {m: i for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split(), 1)}
_DAYS = {d: i for i, d in enumerate("sun mon tue wed thu fri sat".split())}
# (name, lowest, highest, names)
_FIELDS = (
    ("minute", 0, 59, {}),
    ("hour", 0, 23, {}),
    ("day of month", 1, 31, {}),
    ("month", 1, 12, _MONTHS),
    ("day of week", 0, 7, _DAYS),
)
# Searching further than this means the expression can (almost) never fire, e.g. 30 February.
_SEARCH_DAYS = 366 * 5


class CronError(ValueError):
    pass


@dataclass(frozen=True)
class Cron:
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]  # 0 = Sunday
    any_day: bool  # day of month is '*'
    any_weekday: bool  # day of week is '*'

    def day_matches(self, local: datetime) -> bool:
        day_ok = local.day in self.days
        weekday_ok = (local.isoweekday() % 7) in self.weekdays
        if self.any_day and self.any_weekday:
            return True
        if self.any_day:
            return weekday_ok
        if self.any_weekday:
            return day_ok
        return day_ok or weekday_ok

    def matches(self, local: datetime) -> bool:
        return (
            local.minute in self.minutes
            and local.hour in self.hours
            and local.month in self.months
            and self.day_matches(local)
        )


def parse(expression: str) -> Cron:
    text = expression.strip().lower()
    text = _MACROS.get(text, text)
    parts = text.split()
    if len(parts) != 5:
        raise CronError("A cron schedule has 5 fields: minute hour day-of-month month day-of-week")
    values = [_field(part, *spec) for part, spec in zip(parts, _FIELDS, strict=True)]
    weekdays = frozenset(d % 7 for d in values[4])  # 7 is Sunday too
    return Cron(
        minutes=values[0],
        hours=values[1],
        days=values[2],
        months=values[3],
        weekdays=weekdays,
        any_day=parts[2] == "*",
        any_weekday=parts[4] == "*",
    )


def _field(text: str, name: str, low: int, high: int, names: dict[str, int]) -> frozenset[int]:
    out: set[int] = set()
    for item in text.split(","):
        match = re.fullmatch(r"(\*|[a-z0-9]+(?:-[a-z0-9]+)?)(?:/(\d+))?", item)
        if not match:
            raise CronError(f"Cannot read the {name} field '{item}'")
        span, step = match.group(1), int(match.group(2) or 1)
        if step < 1:
            raise CronError(f"The {name} step must be at least 1")
        if span == "*":
            start, end = low, high
        elif "-" in span:
            start, end = (_number(v, name, low, high, names) for v in span.split("-"))
        else:
            start = end = _number(span, name, low, high, names)
            if match.group(2):  # "5/10" means from 5 to the end, every 10
                end = high
        if start > end:
            raise CronError(f"The {name} range {span} goes backwards")
        out.update(range(start, end + 1, step))
    return frozenset(out)


def _number(text: str, name: str, low: int, high: int, names: dict[str, int]) -> int:
    value = names.get(text) if not text.isdigit() else int(text)
    if value is None or not low <= value <= high:
        raise CronError(f"The {name} must be between {low} and {high} (got '{text}')")
    return value


def zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise CronError(f"Unknown time zone '{name}'") from exc


def next_times(expression: str, tz: str, after: datetime, count: int = 3) -> list[datetime]:
    """The next `count` fire times after `after`, as UTC datetimes."""
    cron, zone_info = parse(expression), zone(tz)
    out: list[datetime] = []
    local = after.astimezone(zone_info).replace(second=0, microsecond=0, tzinfo=None)
    local += timedelta(minutes=1)
    limit = local + timedelta(days=_SEARCH_DAYS)
    while len(out) < count and local < limit:
        if local.month not in cron.months:
            year, month = (
                (local.year + 1, 1) if local.month == 12 else (local.year, local.month + 1)
            )
            local = local.replace(year=year, month=month, day=1, hour=0, minute=0)
        elif not cron.day_matches(local):
            local = (local + timedelta(days=1)).replace(hour=0, minute=0)
        elif local.hour not in cron.hours:
            local = (local + timedelta(hours=1)).replace(minute=0)
        elif local.minute not in cron.minutes:
            local += timedelta(minutes=1)
        else:
            out.append(local.replace(tzinfo=zone_info).astimezone(UTC))
            local += timedelta(minutes=1)
    return out


def latest_fire(expression: str, tz: str, now: datetime, window: timedelta) -> datetime | None:
    """The most recent fire time in (now - window, now], as UTC; None when there is none."""
    cron, zone_info = parse(expression), zone(tz)
    minute = now.astimezone(UTC).replace(second=0, microsecond=0)
    oldest = now - window
    while minute > oldest:
        if cron.matches(minute.astimezone(zone_info)):
            return minute
        minute -= timedelta(minutes=1)
    return None
