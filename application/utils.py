from typing import (
    Any,
    Mapping,
    TypeVar, 
    Literal,
    Callable, 
    Iterable,
    Hashable,
    Optional,
    ParamSpec,
    overload
    
)
import time
import secrets
import datetime as dt
import functools as ft

UNDERLINE:  str = '_'

P = ParamSpec('P')

T   = TypeVar('T')
R   = TypeVar('R')
TF  = TypeVar('TF', bound=Callable)

TK  = TypeVar('TK', bound=Hashable)
TV  = TypeVar('TV', bound=Hashable)

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

def generate_timestamp() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)
    
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

def invert_dict(data: Mapping[TK, TV], /) -> dict[TV, TK]:
    return {value: key for key, value in data.items() }

def sign_with(signer: Callable[P, Any], /) -> Callable[[Callable[P, T]], Callable[P, T]]:
    return lambda func: func

@overload
def cast(value: Any, dtype: Callable[[Any], T], ignore_null: Literal[True]=True) -> T | None: ...

@overload
def cast(value: Any, dtype: Callable[[Any], T], ignore_null: Literal[False]=False) -> T: ...

def cast(value: Any, dtype: Callable[[Any], T], ignore_null: bool=True) -> T:
    if ignore_null and value is None:
        return None
    return dtype(value)

def with_suffix(text: str, suffix: str, /) -> str:
    return text if text.endswith(suffix) else text + suffix

def with_prefix(text: str, prefix: str, /) -> str:
    return text if text.startswith(prefix) else prefix + text

def lookup(rule: Callable[[T], bool], options: Iterable[T], /) -> T:
    matches = [value for value in filter(rule, options) ]
    matches_count = len(matches)

    if matches_count == 1:
        return matches[0]
    
    if matches_count == 0:
        raise ValueError('No item matches the given rule')
    
    raise ValueError(f'Multiple items ({matches_count}) match the given rule: {matches}')

def switch(value: T, matches: Mapping[T, R], /) -> R:
    return matches[value]


    


     


