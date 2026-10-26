FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /srv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-group benchmark

COPY app ./app
COPY lua ./lua
COPY proto ./proto

ENV PATH="/srv/.venv/bin:$PATH"

EXPOSE 8000
EXPOSE 50051

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
