FROM python:3.12-slim
WORKDIR /service
COPY pyproject.toml requirements.lock ./
COPY app ./app
RUN pip install --no-cache-dir -c requirements.lock .
COPY migrations ./migrations
COPY alembic.ini ./
RUN useradd --uid 10001 --create-home app
USER app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
