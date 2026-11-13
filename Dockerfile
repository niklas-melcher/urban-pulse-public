FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock /app/
RUN uv sync --no-dev --frozen --no-install-project

COPY README.md /app/
COPY src/ /app/src/

# ---- pipeline target ----
FROM base AS pipeline

RUN uv sync --no-dev --frozen

CMD ["uv", "run", "python", "-m", "urban_pulse.main"]

# ---- dashboard target ----
FROM base AS dashboard

RUN uv sync --no-dev --frozen --extra dashboard

EXPOSE 8501
CMD ["uv", "run", "streamlit", "run", "src/urban_pulse/dashboard/app.py", "--server.address=0.0.0.0"]
