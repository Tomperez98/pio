from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from pio.aio import AIOSystem
from pio.scheduler import Computation, Scheduler
from pio.subsystems.echo import EchoCompletion, EchoSubmission, EchoSubsystem
from pio.subsystems.function import FunctionSubsystem

if TYPE_CHECKING:
    from collections.abc import Callable

    from pio.bus import CQE


def foo(string: str) -> Computation[EchoSubmission, EchoCompletion]:
    p = yield EchoSubmission(string)
    v = yield p
    assert isinstance(v, EchoCompletion)
    assert v.data == string
    return v


def bar() -> Computation[Callable[[], str], str]:
    p = yield lambda: "foo"
    v = yield p
    assert v == "foo"
    return v


def test_scheduler() -> None:
    pool = ThreadPoolExecutor()
    aio = AIOSystem(pool)
    aio.attach_subsystem(EchoSubsystem(aio, pool))
    aio.attach_subsystem(FunctionSubsystem(aio, pool))
    aio.start()
    scheduler = Scheduler(aio)

    i = 0
    futures: list[tuple[Future, Any]] = []
    for comp, expected in [
        (foo("foo"), EchoCompletion("foo")),
        (foo("bar"), EchoCompletion("bar")),
        (bar(), "foo"),
    ]:
        futures.append((scheduler.add(comp), expected))
        i += 1

    scheduler.run_until_blocked(0)

    cqes: list[CQE[EchoCompletion]] = []
    while len(cqes) < i:
        cqes.extend(aio.dequeue(i))

    for cqe in cqes:
        cqe.cb(cqe.v)

    scheduler.run_until_blocked(1)

    for f, expected in futures:
        assert f.result() == expected

    aio.shutdown()
