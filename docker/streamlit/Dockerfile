FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_PORT=8501 \
    TRADE_DUCKDB_PATH=/app/warehouse/analytics.duckdb

COPY requirements-streamlit.txt ./requirements-streamlit.txt
RUN pip install --no-cache-dir -r requirements-streamlit.txt

COPY app ./app
COPY warehouse ./warehouse

EXPOSE 8501

CMD ["streamlit", "run", "app/streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501"]
