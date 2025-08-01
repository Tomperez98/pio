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
    raise RuntimeError()


def qux(string: str) -> str:
    raise RuntimeError()



try:
    print(pio.run(bar(10)))
except Exception as e:
    print(f"Error in bar(10): {type(e)}")

try:
    print(pio.run(lambda: qux("hello, world")))
except Exception as e:
    print(f"Error in qux('hello, world'): {type(e)}")

try:
    print(pio.run(foo()))
except Exception as e:
    print(f"Error in foo(): {type(e)}")
