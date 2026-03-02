#include "common.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

static void url_decode_inplace(char *s) {
    char *p = s;
    char *q = s;
    while (*p) {
        if (*p == '+') { *q++ = ' '; p++; continue; }
        if (*p == '%' && isxdigit((unsigned char)p[1]) && isxdigit((unsigned char)p[2])) {
            char hex[3] = {p[1], p[2], 0};
            *q++ = (char)strtol(hex, NULL, 16);
            p += 3;
            continue;
        }
        *q++ = *p++;
    }
    *q = '\0';
}

static void send_response(int fd, const char *body) {
    const char *hdr = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n";
    send(fd, hdr, strlen(hdr), 0);
    send(fd, body, strlen(body), 0);
}

static void send_not_found(int fd) {
    const char *hdr = "HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n";
    send(fd, hdr, strlen(hdr), 0);
}

int main(int argc, char **argv) {
    const char *index_path = NULL;
    const char *docs_path = NULL;
    int port = 8080;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--index") == 0 && i + 1 < argc) index_path = argv[++i];
        else if (strcmp(argv[i], "--docs") == 0 && i + 1 < argc) docs_path = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) port = atoi(argv[++i]);
    }

    if (!index_path || !docs_path) {
        fprintf(stderr, "Usage: %s --index index.tsv --docs docs.tsv [--port 8080]\n", argv[0]);
        return 1;
    }

    HashTable ht; ht_init(&ht, 200003);
    DocList docs; doclist_init(&docs);
    if (!load_docs(docs_path, &docs)) { fprintf(stderr, "Failed to load docs\n"); return 1; }
    if (!load_index(index_path, &ht)) { fprintf(stderr, "Failed to load index\n"); return 1; }

    IntList all_docs; build_all_docs(&docs, &all_docs);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(server_fd, 16) < 0) { perror("listen"); return 1; }

    printf("Web search on port %d\n", port);

    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;

        char buf[4096];
        int n = recv(client_fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) { close(client_fd); continue; }
        buf[n] = '\0';

        char method[8] = {0};
        char path[2048] = {0};
        if (sscanf(buf, "%7s %2047s", method, path) != 2) { close(client_fd); continue; }

        if (strncmp(method, "GET", 3) != 0) { send_not_found(client_fd); close(client_fd); continue; }

        char *qpos = strchr(path, '?');
        char query[2048] = {0};
        if (qpos) {
            *qpos++ = '\0';
            if (strncmp(qpos, "q=", 2) == 0) {
                strncpy(query, qpos + 2, sizeof(query) - 1);
                url_decode_inplace(query);
            }
        }

        StrBuf body; sb_init(&body);
        sb_push(&body, "<html><head><meta charset='utf-8'><title>Search</title></head><body>", strlen("<html><head><meta charset='utf-8'><title>Search</title></head><body>"));
        sb_push(&body, "<h2>Boolean Search</h2>", strlen("<h2>Boolean Search</h2>"));
        sb_push(&body, "<form><input name='q' size='60' value='", strlen("<form><input name='q' size='60' value='"));
        sb_push(&body, query, strlen(query));
        sb_push(&body, "'><input type='submit' value='Search'></form>", strlen("'><input type='submit' value='Search'></form>"));

        if (query[0]) {
            TokenList tokens; tokenize_query(query, &tokens);
            TokenList postfix; to_postfix(&tokens, &postfix);
            IntList result = evaluate_query(&postfix, &ht, &all_docs);

            char tmp[128];
            snprintf(tmp, sizeof(tmp), "<p>Results: %d</p><ol>", result.count);
            sb_push(&body, tmp, strlen(tmp));

            int limit = result.count < 50 ? result.count : 50;
            for (int i = 0; i < limit; ++i) {
                int id = result.items[i];
                const char *url = (id >= 0 && id < docs.count) ? docs.items[id].url : "";
            sb_push(&body, "<li><a href='", strlen("<li><a href='"));
            sb_push(&body, url, strlen(url));
            sb_push(&body, "'>", strlen("'>"));
            sb_push(&body, url, strlen(url));
            sb_push(&body, "</a></li>", strlen("</a></li>"));
        }
            sb_push(&body, "</ol>", strlen("</ol>"));

            il_free(&result);
            tl_free(&tokens);
            tl_free(&postfix);
        }

        sb_push(&body, "</body></html>", strlen("</body></html>"));

        send_response(client_fd, body.data ? body.data : "");
        sb_free(&body);
        close(client_fd);
    }

    il_free(&all_docs);
    ht_free(&ht);
    doclist_free(&docs);

    return 0;
}
