from collections.abc import Callable, Coroutine, Generator
from functools import partial
import inspect
from queue import Queue
import threading
from typing import Any, assert_never
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

_q = Queue[tuple[Generator[Computation[Any], Any, Any], Future, Any | Exception | None]]()


def _enqueue_back(gen: Generator[Computation[Any], Any, Any], f: Future, child_f: Future):
    assert child_f.done()
    try:
        next = child_f.result()
    except Exception as e:
        next = e
    _q.put((gen, f, next))


def _scheduler() -> None:
    while True:
        gen, f, next = _q.get()
        try:
            if isinstance(next, Exception):
                yieldable = gen.throw(next)
            else:
                yieldable = gen.send(next)
        except StopIteration as e:
            f.set_result(e.value)
        except Exception as e:
            f.set_exception(e)
        else:
            if inspect.isgenerator(yieldable):
                child_f = Future()
                _q.put((yieldable, child_f, None))
                child_f.add_done_callback(partial(_enqueue_back, gen, f))
            elif inspect.iscoroutine(yieldable):
                _submit_async(yieldable).add_done_callback(partial(_enqueue_back, gen, f))
            elif inspect.isfunction(yieldable):
                _submit_sync(yieldable).add_done_callback(partial(_enqueue_back, gen, f))


_gen_executor = threading.Thread(target=_scheduler, daemon=True)
_gen_executor.start()


def _submit_async[T](coro: Coroutine[Any, Any, T]) -> Future[T]:
    return asyncio.run_coroutine_threadsafe(coro, _loop)


def _submit_sync[T](fn: Callable[[], T]) -> Future[T]:
    return _sync_executor.submit(fn)


def run[T](comp: Computation[T]) -> T:
    if inspect.isgenerator(comp):
        f = Future()
        _q.put((comp, f, None))
        return f.result()
    if inspect.iscoroutine(comp):
        return _submit_async(comp).result()
    if inspect.isfunction(comp):
        return _submit_sync(comp).result()
    raise RuntimeError(f"comp={comp} is not valid")
