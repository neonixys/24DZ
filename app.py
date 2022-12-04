import os
from typing import Optional, Callable, Iterator

from flask import Flask, request, render_template, Response

from constants import BASE_DIR, DATA_DIR
from utils import dict_of_utils, log_generator

app = Flask(__name__)


# получить параметры query и file_name из request.args, при ошибке вернуть ошибку 400
# проверить, что файла file_name существует в папке DATA_DIR, при ошибке вернуть ошибку 400
# с помощью функционального программирования (функций filter, map), итераторов/генераторов сконструировать запрос
# вернуть пользователю сформированный результат


@app.get('/')
def index() -> str:
    return render_template("index.html")


@app.post("/perform_query")
def perform_query() -> str | Response:
    file_name: str = request.form['file_name']
    cmd1: str = request.form['cmd1']
    value1: str = request.form['value1']
    cmd2: str = request.form['cmd2']
    value2: str = request.form['value2']

    if not all((file_name, cmd1, value1, cmd2, value2)):
        return Response('Не все поля заполнены', status=400)

    if file_name is None:
        file_name = DATA_DIR

    if not os.path.exists(BASE_DIR + '/data/' + file_name):
        return Response('Файл не найден', status=400)

    default_generator = log_generator()

    first_func: Optional[Callable] = dict_of_utils.get(cmd1)
    second_func: Optional[Callable] = dict_of_utils.get(cmd2)

    result: Optional[Iterator[str]] = None
    if first_func is not None:
        result = first_func(param=value1, generator=default_generator)
        if second_func is not None:
            result = second_func(param=value2, generator=result)

    return render_template("block.html", items=result)


if __name__ == '__main__':
    app.run()
