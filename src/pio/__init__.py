from collections.abc import Callable, Coroutine, Generator
import inspect
import threading
from typing import Any
import asyncio
from concurrent.futures import Future, ThreadPoolExecutor


type Computation[R] = Generator[Computation[Any], Any, R] | Coroutine[Any, Any, R] | Callable[[], R]


def typesafe[T](y: Computation[T]) -> Generator[Computation[T], T, T]:
    return (yield y)


def _asyncio_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


_loop = asyncio.new_event_loop()
_async_executor = threading.Thread(target=_asyncio_loop, args=(_loop,), daemon=True)
_async_executor.start()

_sync_executor = ThreadPoolExecutor()


def _submit_async[T](coro: Coroutine[Any, Any, T]) -> Future[T]:
    return asyncio.run_coroutine_threadsafe(coro, _loop)


def _submit_sync[T](fn: Callable[[], T]) -> Future[T]:
    return _sync_executor.submit(fn)

def run[T](comp: Computation[T]) -> T:
    if inspect.isgenerator(comp):
        raise NotImplementedError()
    if inspect.iscoroutine(comp):
        return _submit_async(comp).result()
    if inspect.isfunction(comp):
        return _submit_sync(comp).result()
    raise RuntimeError(f"comp={comp} is not valid")
