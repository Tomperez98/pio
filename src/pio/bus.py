from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pio.typing import Kind

if TYPE_CHECKING:
    from collections.abc import Callable


type Callback[O] = Callable[[O | Any | Exception], None]


@dataclass(frozen=True)
class SQE[I: Kind | Callable[[], Any], O: Kind | Any]:
    v: I
    cb: Callback[O]


@dataclass(frozen=True)
class CQE[O: Kind | Any]:
    v: O | Exception
    cb: Callback[O]
