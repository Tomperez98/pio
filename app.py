from __future__ import annotations

import pio


def foo() -> pio.Computation[str]:
    v = yield from pio.typesafe(bar(5))
    return v


def bar(n: int) -> pio.Computation[int]:
    yield from pio.typesafe(baz("hello, world!"))
    yield from pio.typesafe(lambda: qux("hello, world!"))
    return n


async def baz(string: str) -> str:
    return string


def qux(string: str) -> str:
    return string


print(pio.run(baz("hello, world")))
print(pio.run(lambda: qux("hello, world")))
print(pio.run(foo()))
