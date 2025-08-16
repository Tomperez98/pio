from __future__ import annotations

import time
from collections.abc import Callable
from threading import Event, Thread
from typing import TYPE_CHECKING, Any

from pio.scheduler import Computation, Scheduler
from pio.typing import AIO, Kind

if TYPE_CHECKING:
    from concurrent.futures import Future


class Pio:
    def __init__(
        self, aio: AIO, size: int = 100, dequeue_size: int = 100, tick_freq: float = 0.1
    ) -> None:
        self._aio = aio
        self._scheduler = Scheduler(aio, size)
        self._dequeue_size = dequeue_size
        self._tick_freq = tick_freq

        self._thread = Thread(target=self._loop, daemon=True)
        self._stop = Event()
        self._stopped = Event()
        self._stopped.set()

    def add[I: Kind | Callable[[], Any], O: Kind | Any](self, comp: Computation[I, O]) -> Future[O]:
        return self._scheduler.add(comp)

    def shutdown(self) -> None:
        self._stop.set()
        self._stopped.wait()
        self._thread.join()
        self._scheduler.shutdown()
        self._stopped.set()
        self._stop.clear()

    def start(self) -> None:
        if self._stopped.is_set():
            self._stopped.clear()
            self._aio.start()
            self._thread.start()

    def _loop(self) -> None:
        while True:
            self.tick(int(time.time() * 1_000))

            if self._stop.wait(self._tick_freq) and self._scheduler.size() == 0:
                self._stopped.set()
                return

    def tick(self, time: int) -> None:
        for cqe in self._aio.dequeue(self._dequeue_size):
            cqe.cb(cqe.v)

        self._scheduler.run_until_blocked(time)
        self._aio.flush(time)
