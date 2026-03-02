FROM debian:bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY src /app/src
COPY CMakeLists.txt /app/CMakeLists.txt
RUN cmake -S /app -B /app/build && cmake --build /app/build

ENTRYPOINT ["/bin/sh"]
