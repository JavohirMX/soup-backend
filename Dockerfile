FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.10.8 /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "--frozen", "--no-dev", "uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
