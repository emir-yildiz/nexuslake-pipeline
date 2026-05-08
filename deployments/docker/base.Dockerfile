# 1. Aşama: Python ve Java Gereksinimleri
FROM python:3.11

# Sistem bağımlılıklarını yükle
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    procps \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Java Home ayarları
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Spark'ın Kafka paketlerini otomatik indirmesi için gerekli ortam değişkeni
# Bu satır Airflow'daki "Failed to find data source: kafka" hatasını çözer.
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 pyspark-shell"

WORKDIR /app

# 2. Aşama: Bağımlılıkların Yüklenmesi
# Önce sadece requirements.txt kopyalanır (Docker Cache avantajı için)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Aşama: Kodların Kopyalanması
COPY . .

# Python path ve çalışma ortamı
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Varsayılan komut (Compose içinde ezilecek)
CMD ["python"]