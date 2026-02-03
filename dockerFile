FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV PYTHONUNBUFFERED=1

# Expose для документации, uvicorn сам откроет порт
EXPOSE 8080

# Запускаем на 8000 порту (стандарт FastAPI)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]