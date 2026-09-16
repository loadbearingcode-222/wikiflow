# The dashboard gets its own small image. Streamlit and Altair have no business inside
# the Airflow image, and keeping them out means a dashboard dependency can never break
# the scheduler's resolver.
FROM python:3.12-slim

# uv pinned to the version the project is developed with.
COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /bin/uv

WORKDIR /app
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1

# Dependencies first, for layer caching: this layer only rebuilds when the lockfile does.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --group dashboard

COPY .streamlit ./.streamlit
COPY dashboard ./dashboard

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["uv", "run", "--frozen", "--no-sync", "streamlit", "run", "dashboard/app.py", \
     "--server.port", "8501", "--server.address", "0.0.0.0"]
