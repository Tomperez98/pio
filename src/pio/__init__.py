from collections.abc import Callable, Generator
from typing import Any
from concurrent.futures import Future

type Computation[R] = Generator[Computation[Any, Any] | Callable[[], Any], Any, R] | Callable[[], R]


def typesafe[T](y: Computation[T]) -> Generator[Computation[T], T, T]:
    return (yield y)

def run[T](comp: Computation[T]) -> Future[T]:...
