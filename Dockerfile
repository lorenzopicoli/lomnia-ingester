FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY uv.lock pyproject.toml /app/
RUN uv sync --frozen --no-install-project

COPY . /app
RUN uv sync --frozen

RUN apt-get update && apt-get install -y supervisor && apt-get install -y git
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD ["supervisord", "-n"]
