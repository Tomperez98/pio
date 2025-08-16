from __future__ import annotations

import contextlib
import queue
from collections import deque
from collections.abc import Callable, Generator
from concurrent.futures import Future
from typing import Any, assert_never

from pio.bus import SQE
from pio.typing import AIO, Kind


class Promise: ...


type Yieldable[I: Kind | Callable[[], Any], O: Kind | Any] = Computation[I, O] | Promise | I
type Computation[I: Kind | Callable[[], Any], O: Kind | Any] = Generator[Yieldable[I, O], Any, O]


class _FinalValue:
    def __init__(self, v: Any | Exception) -> None:
        self.v = v


class _InternalComputation:
    def __init__(self, comp: Computation) -> None:
        self.comp = comp
        self.next: Any | Exception | Promise | None = None
        self.final: _FinalValue | None = None

        self._pend: list[Promise] = []
        self._final: _FinalValue | None = None

    def send(self) -> Yieldable | _FinalValue:
        if self._final is not None:
            if self._pend:
                return self._pend.pop()
            return self._final

        try:
            match self.next:
                case Exception():
                    yielded = self.comp.throw(self.next)
                case Promise():
                    self._pend.append(self.next)
                    yielded = self.comp.send(self.next)
                case _:
                    yielded = self.comp.send(self.next)
        except StopIteration as e:
            yielded = _FinalValue(e.value)
        except Exception as e:
            yielded = _FinalValue(e)

        match yielded:
            case Promise():
                with contextlib.suppress(ValueError):
                    self._pend.remove(yielded)
                return yielded
            case _FinalValue():
                self._final = yielded
                if self._pend:
                    return self._pend.pop()
                return self._final
            case _:
                return yielded


class Scheduler:
    def __init__(self, aio: AIO, size: int = 100) -> None:
        self._aio = aio
        self._in = queue.Queue[tuple[_InternalComputation, Future]](size)

        self._running = deque[_InternalComputation | tuple[_InternalComputation, Future]]()
        self._awaiting: dict[_InternalComputation, _InternalComputation] = {}

        self._p_to_comp: dict[Promise, _InternalComputation] = {}
        self._comp_to_f: dict[_InternalComputation, Future] = {}

    def add[I: Kind | Callable[[], Any], O: Kind | Any](self, comp: Computation[I, O]) -> Future[O]:
        f = Future[O]()
        self._in.put_nowait((_InternalComputation(comp), f))
        return f

    def shutdown(self) -> None:
        self._aio.shutdown()
        self._in.shutdown()
        self._in.join()
        assert len(self._running) == 0
        assert len(self._awaiting) == 0
        assert len(self._p_to_comp) == 0
        assert len(self._comp_to_f) == 0

    def run_until_blocked(self, time: int) -> None:
        assert len(self._running) == 0

        qsize = self._in.qsize()
        for _ in range(qsize):
            try:
                e = self._in.get_nowait()
            except queue.Empty:
                return
            self._running.appendleft(e)
            self._in.task_done()

        self.tick(time)
        assert len(self._running) == 0

    def tick(self, time: int) -> None:
        for blocking in list(self._awaiting):
            if blocking.final is None:
                continue

            blocked = self._awaiting.pop(blocking)
            blocked.next = blocking.final.v
            self._running.appendleft(blocked)

        while self.step(time):
            continue

    def step(self, time: int) -> bool:
        try:
            match item := self._running.pop():
                case _InternalComputation():
                    comp = item
                case (_InternalComputation(), Future()):
                    comp, future = item
                    assert comp.next is None
                    self._comp_to_f[comp] = future
                case _:
                    assert_never(item)

        except IndexError:
            return False

        assert comp.final is None
        yielded = comp.send()

        match yielded:
            case Promise():
                match (child_comp := self._p_to_comp.pop(yielded)).final:
                    case None:
                        self._awaiting[child_comp] = comp
                    case _FinalValue(v=v):
                        comp.next = v
                        self._running.appendleft(comp)
                return True

            case _FinalValue():
                self._set(comp, yielded)
                return True

            case Generator():
                child_comp = _InternalComputation(yielded)
                promise = Promise()
                self._p_to_comp[promise] = child_comp
                self._running.appendleft(child_comp)

                comp.next = promise
                self._running.appendleft(comp)
                return True
            case _:
                child_comp = _InternalComputation(yielded)
                promise = Promise()
                self._p_to_comp[promise] = child_comp
                self._aio.dispatch(
                    SQE(yielded, lambda r, comp=child_comp: self._set(comp, _FinalValue(r)))
                )

                comp.next = promise
                self._running.appendleft(comp)
                return True

    def size(self) -> int:
        return len(self._running) + len(self._awaiting) + self._in.qsize()

    def _set(self, comp: _InternalComputation, final_value: _FinalValue) -> None:
        assert comp.final is None
        comp.final = final_value
        if (f := self._comp_to_f.pop(comp, None)) is not None:
            match comp.final.v:
                case Exception():
                    f.set_exception(comp.final.v)
                case _:
                    f.set_result(comp.final.v)
