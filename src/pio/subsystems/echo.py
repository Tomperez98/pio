from __future__ import annotations

import queue
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pio.bus import CQE, SQE

if TYPE_CHECKING:
    from concurrent.futures import Future, ThreadPoolExecutor

    from pio.typing import AIO


_KIND = "echo"


@dataclass(frozen=True)
class EchoSubmission:
    data: str

    @property
    def kind(self) -> str:
        return _KIND


@dataclass(frozen=True)
class EchoCompletion:
    data: str

    @property
    def kind(self) -> str:
        return _KIND


class EchoSubsystem:
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
        self._sq = queue.Queue[SQE[EchoSubmission, EchoCompletion]](size)
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

    def enqueue(self, sqe: SQE[EchoSubmission, EchoCompletion]) -> bool:
        assert sqe.v.kind == _KIND
        try:
            self._sq.put_nowait(sqe)
        except queue.Full:
            return False
        return True

    def flush(self, time: int) -> None:
        return

    def process(self, sqes: list[SQE[EchoSubmission, EchoCompletion]]) -> list[CQE[EchoCompletion]]:
        sqe = sqes[0]
        assert not isinstance(sqe.v, Callable)

        return [
            CQE(
                EchoCompletion(sqe.v.data),
                sqe.cb,
            ),
        ]

    def worker(self) -> None:
        while True:
            try:
                sqe = self._sq.get()
            except queue.ShutDown:
                break
            assert not isinstance(sqe.v, Callable)
            assert sqe.v.kind == self.kind

            self._aio.enqueue((self.process([sqe])[0], self.kind))
            self._sq.task_done()
