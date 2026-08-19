FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Устанавливаем основные зависимости
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Устанавливаем tinkoff-invest БЕЗ зависимостей (игнорируем битую зависимость tinkoff)
RUN pip install --no-cache-dir --no-deps \
    git+https://github.com/RussianInvestments/invest-python.git

COPY . .

CMD ["python", "bot.py"]
