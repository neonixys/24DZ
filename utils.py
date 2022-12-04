import re
from typing import List, Iterator, Any, Callable

from constants import DATA_DIR


def log_generator() -> Iterator:
    with open(DATA_DIR) as file:
        log_sting: List[str] = file.readlines()
        for log in log_sting:
            yield log


def filter_query(param: str, generator: Iterator[str]) -> Iterator[str]:
    return filter(lambda x: param in x, generator)


def map_query(param: int, generator: Iterator[str]) -> Iterator[str]:
    return map(lambda string: string.split()[int(param)], generator)


def unique_query(generator: Iterator[str], *args: Any, **kwargs: Any) -> Iterator[str]:
    list_: List[str] = []
    for string in generator:
        if string not in list_:
            list_.append(string)
            yield string


def sort_query(param: str, generator: Iterator[str]) -> Iterator[str]:
    return iter(sorted(generator, reverse='asc' == param))


def limit_query(param: str, generator: Iterator[str]) -> Iterator[str]:
    counter = 1
    for string in generator:
        if counter > int(param):
            break

        counter += 1

        yield string


def regex_query(param: str, generator: Iterator[str]) -> Iterator[str]:
    pattern: re.Pattern = re.compile(param)
    return filter(lambda x: re.search(pattern, x), generator)


dict_of_utils: dict[str, Callable[..., Iterator[str]]] = {
    'filter': filter_query,
    'map': map_query,
    'unique': unique_query,
    'sort': sort_query,
    'limit': limit_query,
    'regex': regex_query,
}
