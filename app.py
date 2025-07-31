from __future__ import annotations

import pio


def foo() -> pio.Computation[str]:
    v = yield from pio.typesafe(bar(5))
    return v


def bar(n: int) -> pio.Computation[int]:
    v = yield from pio.typesafe(lambda: baz("hello, world!"))
    return n


def baz(string: str) -> str:
    return string


print(pio.run(foo).result())
