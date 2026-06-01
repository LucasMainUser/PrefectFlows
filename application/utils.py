from typing import (
    Any,
    Mapping,
    TypeVar, 
    Literal,
    Callable, 
    Iterable,
    Hashable,
    Optional,
    overload
    
)
import time
import secrets
import datetime as dt
import functools as ft

UNDERLINE:  str = '_'

T   = TypeVar('T')
TF  = TypeVar('TF', bound=Callable)

def type_name(it: Any, /) -> str:
    return getattr(type(it), '__name__')

def cache(func: TF, /) -> TF:
    return ft.cache(func)

def search(it: Iterable[T], condition: Callable[[T], bool], /) -> T:
    matches = filter(condition, it)
    try:
        return next(matches)
    except StopIteration:
        raise ValueError('No element found matching the given condition.')
    
def clear_text(value: str, /) -> str:
    clean = str().join(char for char in str(value) if char.isprintable() )
    return clean.strip()

def fill_whitespaces(value: str, /) -> str:
    return UNDERLINE.join(str(value).strip().split())

def generate_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def generate_timehex_token(nbytes: int, /) -> str:
    token_time = time.time_ns()
    token_random = secrets.token_hex(nbytes)
    return f'{token_time:020d}{token_random}'

def select_keys(data: Mapping[Hashable, Any], *keys: Hashable) -> dict[Hashable, Any]:
    return {key: data[key] for key in keys}

def is_empty_string(data: str, /) -> bool:
    return str(data).strip() == str()

def coalesce(value: Optional[Any], default: Any, /) -> Any:
    return default if value is None else value

@overload
def cast(value: Any, dtype: Callable[[Any], T], ignore_null: Literal[True]=True) -> T | None: ...

@overload
def cast(value: Any, dtype: Callable[[Any], T], ignore_null: Literal[False]=False) -> T: ...

def cast(value: Any, dtype: Callable[[Any], T], ignore_null: bool=True) -> T:
    if value is None:
        return None
    return dtype(value)




     


