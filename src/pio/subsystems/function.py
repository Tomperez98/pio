from __future__ import annotations

import queue
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pio.bus import CQE, SQE

if TYPE_CHECKING:
    from concurrent.futures import Future, ThreadPoolExecutor

    from pio.typing import AIO


_KIND = "function"


class FunctionSubsystem:
    def __init__(
        self,
        aio: AIO,
        pool: ThreadPoolExecutor | None = None,
        size: int = 100,
        workers: int = 1,
    ) -> None:
        assert size > 0, "size must be positive"
        assert workers > 0, "workers must be positive"

        self._aio = aio
        self._pool = pool
        self._sq = queue.Queue[SQE](size)
        self._workers = workers
        self._futures: list[Future[None]] = []

    @property
    def size(self) -> int:
        return self._sq.maxsize

    @property
    def kind(self) -> str:
        return _KIND

    def start(self) -> None:
        assert self._pool is not None
        if len(self._futures) == 0:
            for _ in range(self._workers):
                self._futures.append(self._pool.submit(self.worker))

    def shutdown(self) -> None:
        if len(self._futures) > 0:
            assert len(self._futures) == self._workers
            self._sq.shutdown()
            for f in self._futures:
                assert f.result() is None

            self._futures.clear()
            self._sq.join()

    def enqueue(self, sqe: SQE[Callable[[], Any], Any]) -> bool:
        assert isinstance(sqe.v, Callable)
        try:
            self._sq.put_nowait(sqe)
        except queue.Full:
            return False
        return True

    def flush(self, time: int) -> None:
        return

    def process(self, sqes: list[SQE]) -> list[CQE]:
        assert len(sqes) == 1
        sqe = sqes[0]
        return [CQE(sqe.v(), sqe.cb)]

    def worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except queue.ShutDown:
                break

            self._aio.enqueue((self.process([sqe])[0], self.kind))
            self._sq.task_done()
