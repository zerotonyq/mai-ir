FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    cmake \
    pkg-config \
    libcurl4-openssl-dev \
    libssl-dev \
    libmongoc-dev \
    libbson-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY CMakeLists.txt /app/CMakeLists.txt
COPY src /app/src

RUN cmake -S /app -B /app/build \
    && cmake --build /app/build -j

COPY config.yaml /app/config.yaml

CMD ["/app/build/crawler", "/app/config.yaml"]
