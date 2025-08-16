from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from pio import Pio
from pio.aio import AIOSystem
from pio.subsystems.echo import EchoCompletion, EchoSubmission, EchoSubsystem
from pio.subsystems.function import FunctionSubsystem

if TYPE_CHECKING:
    from collections.abc import Callable

    from pio.scheduler import Computation



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


def test_system() -> None:
    pool = ThreadPoolExecutor()
    aio = AIOSystem(pool)
    aio.attach_subsystem(EchoSubsystem(aio, pool))
    aio.attach_subsystem(FunctionSubsystem(aio, pool))
    system = Pio(aio)

    system.start()
    system.add(foo("foo")).result()
    system.add(foo("bar")).result()
    system.add(bar()).result()

    futures = []
    futures.append(system.add(foo("foo")))
    futures.append(system.add(foo("bar")))
    futures.append(system.add(bar()))

    for f in futures:
        f.result()
    system.shutdown()
