FROM python:3.8-slim-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN ln -sf /usr/local/bin/python3 /usr/bin/python3 || true

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "src.app"]