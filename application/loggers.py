from typing import Optional, Protocol

class LogFunction(Protocol):
    def __call__(self, message: str, /) -> None: ...

def noop_logger(message: str, /) -> None: ...

def resolve_logger(data: Optional[LogFunction]=None, /) -> LogFunction:
    if data is None:
        data = noop_logger
    if not callable(data):
        raise ValueError('Provided logger must be callable.')
    return data

