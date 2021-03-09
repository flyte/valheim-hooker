FROM python:3.8

RUN pip install poetry

RUN mkdir /app && useradd -d /app hooker && chown -R hooker /app
USER hooker
WORKDIR /app

COPY pyproject.toml poetry.lock valheim_hooker.py /app/

RUN poetry install --no-dev

CMD [ "poetry", "run", "python", "valheim_hooker.py" ]
