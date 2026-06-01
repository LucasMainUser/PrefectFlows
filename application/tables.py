from typing import (
    Any,
    Mapping,
    TypeAlias,
    Optional,
    Callable, 
    Iterable,
    Hashable
)
import polars as pl

PolarsLike:     TypeAlias = Any
TargetValue:    TypeAlias = Any
ColumnName:     TypeAlias = str

DEFAULT = object()

def transform_dataframe(data: PolarsLike, /) -> pl.DataFrame:
    if isinstance(data, pl.DataFrame):
        return data
    try:
        return pl.DataFrame(data, schema=None)
    except Exception as error:
        raise RuntimeError('...') from error

def to_expr(data: pl.Expr | ColumnName, /) -> pl.Expr:
    if isinstance(data, pl.Expr):
        return data
    return pl.col(data)

def list_column_selection(data: ColumnName | Iterable[ColumnName], /) -> list[ColumnName]:
    if isinstance(data, str):
        return [data]
    return list(data)

def any_of(*conditions: pl.Expr) -> pl.Expr:
    output, *others = conditions

    for condition in others:
        output = output | condition

    return output

def select_renaming(data: PolarsLike, renamer: Mapping[ColumnName, str], /) -> pl.DataFrame:
    columns = list(renamer.keys())
    subsets = transform_dataframe(data).select(columns)
    subsets = subsets.rename(renamer, strict=True)
    return subsets

def select_casting(data: PolarsLike, casting: Mapping[ColumnName, pl.DataType], /) -> pl.DataFrame:
    columns = list(casting.keys())
    subsets = transform_dataframe(data).select(columns)
    subsets = subsets.cast(casting, strict=True)
    return subsets

def lookup(data: PolarsLike, /, matches: Mapping[ColumnName, TargetValue], at_column: Optional[ColumnName]=None) -> dict[ColumnName, Any] | Any:
    filter_context = None

    for column_name, value in matches.items():
        condition = pl.col(column_name) == pl.lit(value)
        
        if filter_context is None:
            filter_context = condition
            continue

        filter_context = filter_context & condition
    
    dataframe = transform_dataframe(data).filter(filter_context)

    if dataframe.height == 0:
        raise ValueError('No rows found matching the specified conditions.')
    
    if dataframe.height > 1:
        raise ValueError('Multiple rows found matching the specified conditions; expected exactly one.')
    
    record = dataframe.row(0, named=True)

    if at_column is None:
        return record
    
    return record[at_column]

def first_row_as_header(data: PolarsLike, /) -> pl.DataFrame: 
    dataframe = transform_dataframe(data)
    new_headers = dataframe.row(0, named=False)

    dataframe = dataframe.slice(1)
    dataframe.columns = [str(value) for value in new_headers]
    
    return dataframe

def transform_headers(data: PolarsLike, func: Callable[[str], str], /) -> pl.DataFrame:
    dataframe = transform_dataframe(data)
    dataframe.columns = [func(name) for name in dataframe.columns]
    return dataframe

def skip_rows(data: PolarsLike, num: int, /) -> pl.DataFrame:
    return transform_dataframe(data).slice(num)

def remove_null_rows(
        data: PolarsLike,
        /, 
        subsets: Optional[ColumnName | Iterable[ColumnName]]=None,
        empty_string_is_null: bool=False
    ) -> pl.DataFrame:

    dataframe = transform_dataframe(data)

    if subsets is None:
        subsets = dataframe.columns
    subsets = list_column_selection(subsets)

    conditions = []
    
    for name in subsets:
        condition = pl.col(name).is_not_null()
        
        if empty_string_is_null:
            condition = condition & ( pl.col(name).str.strip_chars() != '' )
        
        conditions.append(condition)

    not_null_rows = pl.all_horizontal(conditions)
    return dataframe.filter(not_null_rows)

def forward_fill(data: PolarsLike, /, subsets: Optional[ColumnName | Iterable[ColumnName]]=None) -> pl.DataFrame:
    dataframe = transform_dataframe(data)

    if subsets is None:
        subsets = dataframe.columns
    subsets = list_column_selection(subsets)
    exprs = (
        pl.col(name).forward_fill().alias(name) for name in subsets
    )
    return dataframe.with_columns(exprs)

def map_elements(
        column: pl.Expr | ColumnName, 
        mapper: Mapping[Hashable, Any],
        /, 
        return_dtype: pl.DataType, 
        strict: bool=True, 
        skip_nulls: bool=True,
        default: Optional[Any]=None
    ) -> pl.Expr:

    def map_rule(key: Any, /) -> Any:
        if strict:
            return mapper[key]
        return mapper.get(key, default)

    return to_expr(column).map_elements(map_rule, return_dtype=return_dtype, skip_nulls=skip_nulls)


def nullify(column: pl.Expr | ColumnName, conditions: pl.Expr | Iterable[pl.Expr], /) -> pl.Expr:
    column = to_expr(column)
    mask = any_of(conditions)
    return pl.when(mask).then(None).otherwise(column)

def conditional_replace(column: pl.Expr | ColumnName, conditions: pl.Expr | Iterable[pl.Expr], /, fallback: Optional[Any]=None) -> pl.Expr:
    column = to_expr(column)
    mask = any_of(conditions)
    return pl.when(mask).then(pl.lit(fallback)).otherwise(column)

def replace_whitespaces(column: pl.Expr | ColumnName, /, replacer: Optional[str]=None) -> pl.Expr:
    if replacer is None:
        replacer = '_'
    return (
        to_expr(column)
        .str.replace_all(r'\s+', replacer)
        .str.strip_chars()
    )

def lowercase_col(column: ColumnName | pl.Expr, /) -> pl.Expr:
    return to_expr(column).str.to_lowercase()

def sanitize_text(column: ColumnName | pl.Expr, /) -> pl.Expr:
    output = (
        to_expr(column)
        .str.replace_all(r'\s+', ' ')
        .str.replace_all(r'[\u0000-\u001F\u007F]', '')
        .str.strip_chars()
    )
    return output