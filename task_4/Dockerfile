FROM debian:bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config libmongoc-dev libbson-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY src /app/src
COPY CMakeLists.txt /app/CMakeLists.txt
RUN cmake -S /app -B /app/build && cmake --build /app/build

COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

ENTRYPOINT ["/app/run.sh"]
