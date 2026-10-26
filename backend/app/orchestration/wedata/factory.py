from collections.abc import Awaitable, Callable

import anyio
import anyio.from_thread

from app.orchestration.wedata.base import WeDataAdapter, WeDataAdapterConfig
from app.orchestration.wedata.client import LiveWeDataAdapter
from app.orchestration.wedata.mock import MockWeDataAdapter


def build_adapter(*, mock: bool, config: WeDataAdapterConfig) -> WeDataAdapter:
    if mock:
        return MockWeDataAdapter(config)
    return LiveWeDataAdapter(config)


def run_async[T](fn: Callable[[], Awaitable[T]]) -> T:
    """Run an adapter coroutine from sync code."""
    try:
        return anyio.from_thread.run(fn)
    except anyio.NoEventLoopError:
        return anyio.run(fn)
