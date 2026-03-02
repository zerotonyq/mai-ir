FROM gcc:13-bookworm

WORKDIR /app

COPY src/main.c /app/src/main.c
RUN gcc -O2 -std=c11 -o /app/tokenize /app/src/main.c -lm

COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

ENTRYPOINT ["/app/run.sh"]
