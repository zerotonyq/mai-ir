#include <bson/bson.h>
#include <mongoc/mongoc.h>

#include <curl/curl.h>
#include <openssl/sha.h>

#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    char** items;
    int count;
    int cap;
} StrList;

typedef struct {
    char* name;
    StrList urls;
} Source;

typedef struct {
    Source* items;
    int count;
    int cap;
} SourceList;

typedef struct {
    char* mongo_uri;
    char* database;
    char* pages_collection;
    char* queue_collection;

    int delay_ms;
    int revisit_seconds;
    int revisit_scan_seconds;
    int request_timeout_seconds;
    int follow_links;
    int max_depth;
    int max_links_per_page;
    char* user_agent;
    int articles_only;

    StrList allow_domains;
    StrList deny_exts;
    SourceList sources;
} Config;

static volatile sig_atomic_t g_stop = 0;

static void signal_handler(int sig) {
    (void)sig;
    g_stop = 1;
}

static long long unix_timestamp(void) {
    return (long long)time(NULL);
}

static void strlist_init(StrList* list) {
    list->items = NULL;
    list->count = 0;
    list->cap = 0;
}

static void strlist_free(StrList* list) {
    if (!list) return;
    for (int i = 0; i < list->count; ++i) {
        free(list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->cap = 0;
}

static int strlist_add(StrList* list, const char* value) {
    if (!list || !value) return 0;
    if (list->count == list->cap) {
        int new_cap = list->cap == 0 ? 8 : list->cap * 2;
        char** next = (char**)realloc(list->items, sizeof(char*) * new_cap);
        if (!next) return 0;
        list->items = next;
        list->cap = new_cap;
    }
    size_t len = strlen(value);
    char* copy = (char*)malloc(len + 1);
    if (!copy) return 0;
    memcpy(copy, value, len + 1);
    list->items[list->count++] = copy;
    return 1;
}

static void sourcelist_init(SourceList* list) {
    list->items = NULL;
    list->count = 0;
    list->cap = 0;
}

static void sourcelist_free(SourceList* list) {
    if (!list) return;
    for (int i = 0; i < list->count; ++i) {
        free(list->items[i].name);
        strlist_free(&list->items[i].urls);
    }
    free(list->items);
    list->items = NULL;
    list->count = 0;
    list->cap = 0;
}

static Source* sourcelist_add(SourceList* list) {
    if (!list) return NULL;
    if (list->count == list->cap) {
        int new_cap = list->cap == 0 ? 4 : list->cap * 2;
        Source* next = (Source*)realloc(list->items, sizeof(Source) * new_cap);
        if (!next) return NULL;
        list->items = next;
        list->cap = new_cap;
    }
    Source* src = &list->items[list->count++];
    src->name = NULL;
    strlist_init(&src->urls);
    return src;
}

static void config_init(Config* cfg) {
    memset(cfg, 0, sizeof(Config));
    cfg->delay_ms = 1000;
    cfg->revisit_seconds = 86400;
    cfg->revisit_scan_seconds = 300;
    cfg->request_timeout_seconds = 20;
    cfg->follow_links = 0;
    cfg->max_depth = 1;
    cfg->max_links_per_page = 50;
    cfg->user_agent = NULL;
    cfg->articles_only = 0;
    strlist_init(&cfg->allow_domains);
    strlist_init(&cfg->deny_exts);
    sourcelist_init(&cfg->sources);
}

static void config_free(Config* cfg) {
    free(cfg->mongo_uri);
    free(cfg->database);
    free(cfg->pages_collection);
    free(cfg->queue_collection);
    free(cfg->user_agent);
    strlist_free(&cfg->allow_domains);
    strlist_free(&cfg->deny_exts);
    sourcelist_free(&cfg->sources);
}

static char* trim_inplace(char* s) {
    while (*s && isspace((unsigned char)*s)) s++;
    if (*s == 0) return s;
    char* end = s + strlen(s) - 1;
    while (end > s && isspace((unsigned char)*end)) {
        *end = '\0';
        end--;
    }
    return s;
}

static void strip_quotes_inplace(char* s) {
    size_t len = strlen(s);
    if (len >= 2) {
        if ((s[0] == '"' && s[len - 1] == '"') || (s[0] == '\'' && s[len - 1] == '\'')) {
            memmove(s, s + 1, len - 2);
            s[len - 2] = '\0';
        }
    }
}

static int starts_with(const char* s, const char* prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

static char* dup_str(const char* s) {
    size_t len = strlen(s);
    char* out = (char*)malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, s, len + 1);
    return out;
}

static void parse_key_value(const char* line, char* key, size_t key_cap, char* value, size_t val_cap) {
    const char* colon = strchr(line, ':');
    if (!colon) {
        key[0] = '\0';
        value[0] = '\0';
        return;
    }
    size_t klen = (size_t)(colon - line);
    if (klen >= key_cap) klen = key_cap - 1;
    memcpy(key, line, klen);
    key[klen] = '\0';

    const char* v = colon + 1;
    while (*v && isspace((unsigned char)*v)) v++;
    strncpy(value, v, val_cap - 1);
    value[val_cap - 1] = '\0';
}

static int parse_config(const char* path, Config* cfg) {
    FILE* f = fopen(path, "r");
    if (!f) return 0;

    enum { SEC_NONE, SEC_DB, SEC_LOGIC, SEC_SOURCES } section = SEC_NONE;
    int in_allow_domains = 0;
    int in_deny_exts = 0;
    int in_urls = 0;
    Source* current_source = NULL;

    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        char* p = line;
        char* comment = strchr(p, '#');
        if (comment) *comment = '\0';
        if (*p == '\0') continue;

        int indent = 0;
        while (p[indent] == ' ') indent++;
        p += indent;
        p = trim_inplace(p);
        if (*p == '\0') continue;

        if (indent == 0) {
            in_allow_domains = 0;
            in_deny_exts = 0;
            in_urls = 0;
            if (starts_with(p, "db:")) section = SEC_DB;
            else if (starts_with(p, "logic:")) section = SEC_LOGIC;
            else if (starts_with(p, "sources:")) section = SEC_SOURCES;
            else section = SEC_NONE;
            continue;
        }

        if (section == SEC_DB && indent >= 2) {
            char key[128], val[2048];
            parse_key_value(p, key, sizeof(key), val, sizeof(val));
            char* k = trim_inplace(key);
            char* v = trim_inplace(val);
            strip_quotes_inplace(v);
            if (strcmp(k, "uri") == 0) { free(cfg->mongo_uri); cfg->mongo_uri = dup_str(v); }
            else if (strcmp(k, "database") == 0) { free(cfg->database); cfg->database = dup_str(v); }
            else if (strcmp(k, "pages_collection") == 0) { free(cfg->pages_collection); cfg->pages_collection = dup_str(v); }
            else if (strcmp(k, "queue_collection") == 0) { free(cfg->queue_collection); cfg->queue_collection = dup_str(v); }
            continue;
        }

        if (section == SEC_LOGIC && indent >= 2) {
            if (starts_with(p, "-") && in_allow_domains) {
                char* v = p + 1;
                v = trim_inplace(v);
                strip_quotes_inplace(v);
                strlist_add(&cfg->allow_domains, v);
                continue;
            }
            if (starts_with(p, "-") && in_deny_exts) {
                char* v = p + 1;
                v = trim_inplace(v);
                strip_quotes_inplace(v);
                strlist_add(&cfg->deny_exts, v);
                continue;
            }
            char key[128], val[2048];
            parse_key_value(p, key, sizeof(key), val, sizeof(val));
            char* k = trim_inplace(key);
            char* v = trim_inplace(val);
            strip_quotes_inplace(v);
            if (strcmp(k, "delay_ms") == 0) cfg->delay_ms = atoi(v);
            else if (strcmp(k, "revisit_seconds") == 0) cfg->revisit_seconds = atoi(v);
            else if (strcmp(k, "revisit_scan_seconds") == 0) cfg->revisit_scan_seconds = atoi(v);
            else if (strcmp(k, "request_timeout_seconds") == 0) cfg->request_timeout_seconds = atoi(v);
            else if (strcmp(k, "follow_links") == 0) cfg->follow_links = (strcmp(v, "true") == 0 || strcmp(v, "1") == 0);
            else if (strcmp(k, "max_depth") == 0) cfg->max_depth = atoi(v);
            else if (strcmp(k, "max_links_per_page") == 0) cfg->max_links_per_page = atoi(v);
            else if (strcmp(k, "user_agent") == 0) { free(cfg->user_agent); cfg->user_agent = dup_str(v); }
            else if (strcmp(k, "allow_domains") == 0) {
                in_allow_domains = 1;
                in_deny_exts = 0;
            } else if (strcmp(k, "deny_exts") == 0) {
                in_deny_exts = 1;
                in_allow_domains = 0;
            } else if (strcmp(k, "articles_only") == 0) {
                cfg->articles_only = (strcmp(v, "true") == 0 || strcmp(v, "1") == 0);
            } else {
                in_allow_domains = 0;
                in_deny_exts = 0;
            }
            continue;
        }

        if (section == SEC_SOURCES && indent >= 2) {
            if (starts_with(p, "- name:")) {
                in_urls = 0;
                current_source = sourcelist_add(&cfg->sources);
                if (!current_source) continue;
                char* v = p + strlen("- name:");
                v = trim_inplace(v);
                strip_quotes_inplace(v);
                current_source->name = dup_str(v);
                continue;
            }
            if (starts_with(p, "urls:")) {
                in_urls = 1;
                continue;
            }
            if (in_urls && starts_with(p, "-")) {
                if (!current_source) continue;
                char* v = p + 1;
                v = trim_inplace(v);
                strip_quotes_inplace(v);
                strlist_add(&current_source->urls, v);
                continue;
            }
        }
    }

    fclose(f);
    if (!cfg->pages_collection) cfg->pages_collection = dup_str("pages");
    if (!cfg->queue_collection) cfg->queue_collection = dup_str("queue");
    if (!cfg->user_agent) cfg->user_agent = dup_str("infosearch-crawler/1.0");
    return 1;
}

static void tolower_inplace(char* s) {
    while (*s) {
        *s = (char)tolower((unsigned char)*s);
        s++;
    }
}

static char* normalize_url(const char* url) {
    CURLU* h = curl_url();
    if (!h) return dup_str(url);
    if (curl_url_set(h, CURLUPART_URL, url, CURLU_GUESS_SCHEME) != CURLUE_OK) {
        curl_url_cleanup(h);
        return dup_str(url);
    }

    char* scheme = NULL;
    char* host = NULL;
    char* path = NULL;
    char* port = NULL;

    curl_url_get(h, CURLUPART_SCHEME, &scheme, 0);
    curl_url_get(h, CURLUPART_HOST, &host, 0);
    curl_url_get(h, CURLUPART_PATH, &path, 0);
    curl_url_get(h, CURLUPART_PORT, &port, 0);

    if (scheme) {
        tolower_inplace(scheme);
        curl_url_set(h, CURLUPART_SCHEME, scheme, 0);
    }
    if (host) {
        tolower_inplace(host);
        curl_url_set(h, CURLUPART_HOST, host, 0);
    }
    if (!path) {
        curl_url_set(h, CURLUPART_PATH, "/", 0);
    }

    if (scheme && port) {
        if ((strcmp(scheme, "http") == 0 && strcmp(port, "80") == 0) ||
            (strcmp(scheme, "https") == 0 && strcmp(port, "443") == 0)) {
            curl_url_set(h, CURLUPART_PORT, NULL, 0);
        }
    }

    char* out = NULL;
    curl_url_get(h, CURLUPART_URL, &out, 0);
    char* normalized = out ? dup_str(out) : dup_str(url);

    curl_free(scheme);
    curl_free(host);
    curl_free(path);
    curl_free(port);
    curl_free(out);
    curl_url_cleanup(h);

    return normalized;
}

static int domain_allowed(const char* url, const StrList* allow_domains) {
    if (!allow_domains || allow_domains->count == 0) return 1;

    CURLU* h = curl_url();
    if (!h) return 0;
    if (curl_url_set(h, CURLUPART_URL, url, CURLU_GUESS_SCHEME) != CURLUE_OK) {
        curl_url_cleanup(h);
        return 0;
    }

    char* host = NULL;
    curl_url_get(h, CURLUPART_HOST, &host, 0);
    if (!host) {
        curl_url_cleanup(h);
        return 0;
    }
    tolower_inplace(host);

    for (int i = 0; i < allow_domains->count; ++i) {
        const char* dom = allow_domains->items[i];
        size_t hlen = strlen(host);
        size_t dlen = strlen(dom);
        if (hlen == dlen && strcasecmp(host, dom) == 0) {
            curl_free(host);
            curl_url_cleanup(h);
            return 1;
        }
        if (hlen > dlen) {
            const char* tail = host + (hlen - dlen);
            if (strcasecmp(tail, dom) == 0 && host[hlen - dlen - 1] == '.') {
                curl_free(host);
                curl_url_cleanup(h);
                return 1;
            }
        }
    }

    curl_free(host);
    curl_url_cleanup(h);
    return 0;
}

static int ends_with_ci(const char* s, const char* suf) {
    size_t sl = strlen(s);
    size_t sufl = strlen(suf);
    if (sufl > sl) return 0;
    return strcasecmp(s + sl - sufl, suf) == 0;
}

static int has_denied_ext(const char* url, const StrList* deny_exts) {
    if (!deny_exts || deny_exts->count == 0) return 0;
    CURLU* h = curl_url();
    if (!h) return 0;
    if (curl_url_set(h, CURLUPART_URL, url, CURLU_GUESS_SCHEME) != CURLUE_OK) {
        curl_url_cleanup(h);
        return 0;
    }
    char* path = NULL;
    curl_url_get(h, CURLUPART_PATH, &path, 0);
    if (!path) {
        curl_url_cleanup(h);
        return 0;
    }
    for (int i = 0; i < deny_exts->count; ++i) {
        if (ends_with_ci(path, deny_exts->items[i])) {
            curl_free(path);
            curl_url_cleanup(h);
            return 1;
        }
    }
    curl_free(path);
    curl_url_cleanup(h);
    return 0;
}

static int is_wikipedia_article(const char* path) {
    if (!starts_with(path, "/wiki/")) return 0;
    const char* title = path + strlen("/wiki/");
    if (*title == '\0') return 0;
    if (strchr(title, ':')) return 0; // skip special namespaces
    return 1;
}

static int is_habr_article(const char* path) {
    if (!starts_with(path, "/ru/articles/")) return 0;
    const char* q = path + strlen("/ru/articles/");
    if (!isdigit((unsigned char)*q)) return 0;
    while (*q && isdigit((unsigned char)*q)) q++;
    if (*q == '/') return 1;
    return 0;
}

static int is_article_url(const char* url) {
    CURLU* h = curl_url();
    if (!h) return 0;
    if (curl_url_set(h, CURLUPART_URL, url, CURLU_GUESS_SCHEME) != CURLUE_OK) {
        curl_url_cleanup(h);
        return 0;
    }
    char* host = NULL;
    char* path = NULL;
    curl_url_get(h, CURLUPART_HOST, &host, 0);
    curl_url_get(h, CURLUPART_PATH, &path, 0);
    if (!host || !path) {
        curl_free(host);
        curl_free(path);
        curl_url_cleanup(h);
        return 0;
    }
    tolower_inplace(host);
    int ok = 0;
    if (ends_with_ci(host, "ru.wikipedia.org")) {
        ok = is_wikipedia_article(path);
    } else if (ends_with_ci(host, "habr.com")) {
        ok = is_habr_article(path);
    }
    curl_free(host);
    curl_free(path);
    curl_url_cleanup(h);
    return ok;
}

static int url_allowed(const char* url, const Config* cfg) {
    if (!domain_allowed(url, &cfg->allow_domains)) return 0;
    if (has_denied_ext(url, &cfg->deny_exts)) return 0;
    return 1;
}

static int url_store_allowed(const char* url, const Config* cfg) {
    if (!cfg->articles_only) return 1;
    return is_article_url(url);
}

typedef struct {
    char* data;
    size_t len;
    size_t cap;
} Buffer;

static size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    Buffer* buf = (Buffer*)userdata;
    size_t total = size * nmemb;
    if (total == 0) return 0;
    if (buf->len + total + 1 > buf->cap) {
        size_t new_cap = buf->cap == 0 ? 8192 : buf->cap * 2;
        while (new_cap < buf->len + total + 1) new_cap *= 2;
        char* next = (char*)realloc(buf->data, new_cap);
        if (!next) return 0;
        buf->data = next;
        buf->cap = new_cap;
    }
    memcpy(buf->data + buf->len, ptr, total);
    buf->len += total;
    buf->data[buf->len] = '\0';
    return total;
}

typedef struct {
    long status;
    char* body;
    char* final_url;
    int ok;
} HttpResult;

static HttpResult fetch_url(const char* url, const Config* cfg) {
    HttpResult result;
    result.status = 0;
    result.body = NULL;
    result.final_url = NULL;
    result.ok = 0;

    CURL* curl = curl_easy_init();
    if (!curl) return result;

    Buffer buf;
    buf.data = NULL;
    buf.len = 0;
    buf.cap = 0;

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, cfg->user_agent ? cfg->user_agent : "infosearch-crawler/1.0");
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, cfg->request_timeout_seconds);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

    CURLcode res = curl_easy_perform(curl);
    if (res == CURLE_OK) {
        long status = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
        char* eff = NULL;
        curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &eff);
        result.status = status;
        if (!buf.data) {
            buf.data = (char*)malloc(1);
            if (buf.data) buf.data[0] = '\0';
        }
        result.body = buf.data;
        if (eff) result.final_url = dup_str(eff);
        result.ok = 1;
    } else {
        free(buf.data);
    }

    curl_easy_cleanup(curl);
    return result;
}

static char* sha256_hex(const char* data, size_t len) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char*)data, len, hash);
    static const char* hex = "0123456789abcdef";
    char* out = (char*)malloc(SHA256_DIGEST_LENGTH * 2 + 1);
    if (!out) return NULL;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        out[i * 2] = hex[(hash[i] >> 4) & 0xF];
        out[i * 2 + 1] = hex[hash[i] & 0xF];
    }
    out[SHA256_DIGEST_LENGTH * 2] = '\0';
    return out;
}

static int match_word_ci(const char* s, const char* word) {
    for (int i = 0; word[i]; ++i) {
        if (s[i] == '\0') return 0;
        if (tolower((unsigned char)s[i]) != tolower((unsigned char)word[i])) return 0;
    }
    return 1;
}

static void extract_links(const char* html, int max_links, StrList* out) {
    const char* p = html;
    while (*p) {
        if ((*p == 'h' || *p == 'H') && match_word_ci(p, "href")) {
            const char* q = p + 4;
            while (*q && isspace((unsigned char)*q)) q++;
            if (*q == '=') {
                q++;
                while (*q && isspace((unsigned char)*q)) q++;
                if (*q == '"' || *q == '\'') {
                    char quote = *q;
                    q++;
                    const char* start = q;
                    while (*q && *q != quote) q++;
                    if (*q == quote) {
                        size_t len = (size_t)(q - start);
                        if (len > 0 && len < 2048) {
                            char tmp[2048];
                            memcpy(tmp, start, len);
                            tmp[len] = '\0';
                            if (starts_with(tmp, "http://") || starts_with(tmp, "https://")) {
                                strlist_add(out, tmp);
                                if (out->count >= max_links) return;
                            }
                        }
                    }
                }
            }
        }
        p++;
    }
}

static void ensure_indexes(mongoc_collection_t* pages, mongoc_collection_t* queue) {
    bson_error_t error;
    bson_t keys;
    mongoc_index_opt_t idxopt;

    mongoc_index_opt_init(&idxopt);
    bson_init(&keys);
    BSON_APPEND_INT32(&keys, "url", 1);
    idxopt.unique = true;
    mongoc_collection_create_index_with_opts(pages, &keys, &idxopt, NULL, NULL, &error);
    bson_destroy(&keys);

    mongoc_index_opt_init(&idxopt);
    bson_init(&keys);
    BSON_APPEND_INT32(&keys, "url", 1);
    idxopt.unique = true;
    mongoc_collection_create_index_with_opts(queue, &keys, &idxopt, NULL, NULL, &error);
    bson_destroy(&keys);

    mongoc_index_opt_init(&idxopt);
    bson_init(&keys);
    BSON_APPEND_INT32(&keys, "status", 1);
    mongoc_collection_create_index_with_opts(queue, &keys, &idxopt, NULL, NULL, &error);
    bson_destroy(&keys);
}

static void reset_processing(mongoc_collection_t* queue) {
    bson_t filter;
    bson_t update;
    bson_t set_doc;

    bson_init(&filter);
    BSON_APPEND_UTF8(&filter, "status", "processing");

    bson_init(&update);
    BSON_APPEND_DOCUMENT_BEGIN(&update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "status", "pending");
    bson_append_document_end(&update, &set_doc);

    mongoc_collection_update_many(queue, &filter, &update, NULL, NULL, NULL);
    bson_destroy(&filter);
    bson_destroy(&update);
}

static void seed_queue(mongoc_collection_t* queue, const Config* cfg) {
    for (int i = 0; i < cfg->sources.count; ++i) {
        Source* src = &cfg->sources.items[i];
        for (int j = 0; j < src->urls.count; ++j) {
            if (!url_allowed(src->urls.items[j], cfg)) {
                continue;
            }
            char* normalized = normalize_url(src->urls.items[j]);

            bson_t filter;
            bson_t update;
            bson_t set_on_insert;
            bson_t opts;

            bson_init(&filter);
            BSON_APPEND_UTF8(&filter, "url", normalized);

            bson_init(&update);
            BSON_APPEND_DOCUMENT_BEGIN(&update, "$setOnInsert", &set_on_insert);
            BSON_APPEND_UTF8(&set_on_insert, "url", normalized);
            BSON_APPEND_UTF8(&set_on_insert, "source", src->name ? src->name : "");
            BSON_APPEND_INT32(&set_on_insert, "depth", 0);
            BSON_APPEND_UTF8(&set_on_insert, "status", "pending");
            BSON_APPEND_INT64(&set_on_insert, "enqueued_at", unix_timestamp());
            bson_append_document_end(&update, &set_on_insert);

            bson_init(&opts);
            BSON_APPEND_BOOL(&opts, "upsert", true);

            mongoc_collection_update_one(queue, &filter, &update, &opts, NULL, NULL);

            bson_destroy(&filter);
            bson_destroy(&update);
            bson_destroy(&opts);
            free(normalized);
        }
    }
}

static void enqueue_links(mongoc_collection_t* queue, const StrList* links, const char* source, int depth, const Config* cfg) {
    for (int i = 0; i < links->count; ++i) {
        if (!url_allowed(links->items[i], cfg)) {
            continue;
        }
        char* normalized = normalize_url(links->items[i]);

        bson_t filter;
        bson_t update;
        bson_t set_on_insert;
        bson_t opts;

        bson_init(&filter);
        BSON_APPEND_UTF8(&filter, "url", normalized);

        bson_init(&update);
        BSON_APPEND_DOCUMENT_BEGIN(&update, "$setOnInsert", &set_on_insert);
        BSON_APPEND_UTF8(&set_on_insert, "url", normalized);
        BSON_APPEND_UTF8(&set_on_insert, "source", source ? source : "");
        BSON_APPEND_INT32(&set_on_insert, "depth", depth);
        BSON_APPEND_UTF8(&set_on_insert, "status", "pending");
        BSON_APPEND_INT64(&set_on_insert, "enqueued_at", unix_timestamp());
        bson_append_document_end(&update, &set_on_insert);

        bson_init(&opts);
        BSON_APPEND_BOOL(&opts, "upsert", true);

        mongoc_collection_update_one(queue, &filter, &update, &opts, NULL, NULL);

        bson_destroy(&filter);
        bson_destroy(&update);
        bson_destroy(&opts);
        free(normalized);
    }
}

static void add_due_for_revisit(mongoc_collection_t* pages, mongoc_collection_t* queue, int revisit_seconds) {
    if (revisit_seconds <= 0) return;
    long long cutoff = unix_timestamp() - revisit_seconds;

    bson_t query;
    bson_t lte_doc;
    bson_t opts;

    bson_init(&query);
    BSON_APPEND_DOCUMENT_BEGIN(&query, "fetched_at", &lte_doc);
    BSON_APPEND_INT64(&lte_doc, "$lte", cutoff);
    bson_append_document_end(&query, &lte_doc);

    bson_init(&opts);
    BSON_APPEND_INT64(&opts, "limit", 200);

    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(pages, &query, &opts, NULL);
    const bson_t* doc;
    while (mongoc_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        const char* url = NULL;
        const char* source = NULL;
        if (bson_iter_init_find(&iter, doc, "url") && BSON_ITER_HOLDS_UTF8(&iter)) {
            url = bson_iter_utf8(&iter, NULL);
        }
        if (bson_iter_init_find(&iter, doc, "source") && BSON_ITER_HOLDS_UTF8(&iter)) {
            source = bson_iter_utf8(&iter, NULL);
        }
        if (!url || !source) continue;

        bson_t filter;
        bson_t update;
        bson_t set_on_insert;
        bson_t up_opts;

        bson_init(&filter);
        BSON_APPEND_UTF8(&filter, "url", url);

        bson_init(&update);
        BSON_APPEND_DOCUMENT_BEGIN(&update, "$setOnInsert", &set_on_insert);
        BSON_APPEND_UTF8(&set_on_insert, "url", url);
        BSON_APPEND_UTF8(&set_on_insert, "source", source);
        BSON_APPEND_INT32(&set_on_insert, "depth", 0);
        BSON_APPEND_UTF8(&set_on_insert, "status", "pending");
        BSON_APPEND_BOOL(&set_on_insert, "revisit", true);
        BSON_APPEND_INT64(&set_on_insert, "enqueued_at", unix_timestamp());
        bson_append_document_end(&update, &set_on_insert);

        bson_init(&up_opts);
        BSON_APPEND_BOOL(&up_opts, "upsert", true);

        mongoc_collection_update_one(queue, &filter, &update, &up_opts, NULL, NULL);

        bson_destroy(&filter);
        bson_destroy(&update);
        bson_destroy(&up_opts);
    }

    mongoc_cursor_destroy(cursor);
    bson_destroy(&query);
    bson_destroy(&opts);
}

static int find_one_page(mongoc_collection_t* pages, const char* url, bson_t* out_doc) {
    bson_t query;
    bson_init(&query);
    BSON_APPEND_UTF8(&query, "url", url);

    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(pages, &query, NULL, NULL);
    const bson_t* doc;
    int found = 0;
    if (mongoc_cursor_next(cursor, &doc)) {
        bson_copy_to(doc, out_doc);
        found = 1;
    }

    mongoc_cursor_destroy(cursor);
    bson_destroy(&query);
    return found;
}

static void update_or_insert_page(mongoc_collection_t* pages, const char* url, const char* html, const char* source, const char* hash) {
    bson_t filter;
    bson_t update;
    bson_t set_doc;
    bson_t opts;
    size_t html_len = html ? strlen(html) : 0;

    bson_init(&filter);
    BSON_APPEND_UTF8(&filter, "url", url);

    bson_init(&update);
    BSON_APPEND_DOCUMENT_BEGIN(&update, "$set", &set_doc);
    BSON_APPEND_UTF8(&set_doc, "url", url);
    BSON_APPEND_BINARY(&set_doc, "html", BSON_SUBTYPE_BINARY, (const uint8_t*)html, (uint32_t)html_len);
    BSON_APPEND_UTF8(&set_doc, "source", source ? source : "");
    BSON_APPEND_INT64(&set_doc, "fetched_at", unix_timestamp());
    BSON_APPEND_UTF8(&set_doc, "hash", hash);
    bson_append_document_end(&update, &set_doc);

    bson_init(&opts);
    BSON_APPEND_BOOL(&opts, "upsert", true);
    mongoc_collection_update_one(pages, &filter, &update, &opts, NULL, NULL);
    bson_destroy(&filter);
    bson_destroy(&update);
    bson_destroy(&opts);
}

static void update_last_checked(mongoc_collection_t* pages, const char* url) {
    bson_t filter;
    bson_t update;
    bson_t set_doc;

    bson_init(&filter);
    BSON_APPEND_UTF8(&filter, "url", url);

    bson_init(&update);
    BSON_APPEND_DOCUMENT_BEGIN(&update, "$set", &set_doc);
    BSON_APPEND_INT64(&set_doc, "last_checked", unix_timestamp());
    bson_append_document_end(&update, &set_doc);

    mongoc_collection_update_one(pages, &filter, &update, NULL, NULL, NULL);
    bson_destroy(&filter);
    bson_destroy(&update);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: crawler <config.yaml>\n");
        return 1;
    }

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    Config cfg;
    config_init(&cfg);

    if (!parse_config(argv[1], &cfg)) {
        fprintf(stderr, "Failed to read config: %s\n", argv[1]);
        config_free(&cfg);
        return 1;
    }

    if (!cfg.mongo_uri || !cfg.database) {
        fprintf(stderr, "Config error: db.uri and db.database are required.\n");
        config_free(&cfg);
        return 1;
    }

    mongoc_init();
    mongoc_client_t* client = mongoc_client_new(cfg.mongo_uri);
    if (!client) {
        fprintf(stderr, "Failed to create Mongo client.\n");
        config_free(&cfg);
        mongoc_cleanup();
        return 1;
    }

    mongoc_collection_t* pages = mongoc_client_get_collection(client, cfg.database, cfg.pages_collection);
    mongoc_collection_t* queue = mongoc_client_get_collection(client, cfg.database, cfg.queue_collection);

    ensure_indexes(pages, queue);
    reset_processing(queue);

    bson_t empty;
    bson_init(&empty);
    bson_error_t error;
    int64_t queue_count = mongoc_collection_count_documents(queue, &empty, NULL, NULL, NULL, &error);
    if (queue_count == 0) {
        seed_queue(queue, &cfg);
    }
    bson_destroy(&empty);

    long long last_revisit_scan = 0;

    while (!g_stop) {
        long long now = unix_timestamp();
        if (cfg.revisit_scan_seconds > 0 && (now - last_revisit_scan) >= cfg.revisit_scan_seconds) {
            add_due_for_revisit(pages, queue, cfg.revisit_seconds);
            last_revisit_scan = now;
        }

        bson_t query;
        bson_t update;
        bson_t set_doc;
        bson_t sort;
        bson_t reply;

        bson_init(&query);
        BSON_APPEND_UTF8(&query, "status", "pending");

        bson_init(&update);
        BSON_APPEND_DOCUMENT_BEGIN(&update, "$set", &set_doc);
        BSON_APPEND_UTF8(&set_doc, "status", "processing");
        BSON_APPEND_INT64(&set_doc, "started_at", now);
        bson_append_document_end(&update, &set_doc);

        bson_init(&sort);
        BSON_APPEND_INT32(&sort, "enqueued_at", 1);

        mongoc_find_and_modify_opts_t* opts = mongoc_find_and_modify_opts_new();
        mongoc_find_and_modify_opts_set_update(opts, &update);
        mongoc_find_and_modify_opts_set_sort(opts, &sort);
        mongoc_find_and_modify_opts_set_flags(opts, MONGOC_FIND_AND_MODIFY_RETURN_NEW);

        bson_init(&reply);
        bson_error_t fm_error;
        bool ok = mongoc_collection_find_and_modify_with_opts(queue, &query, opts, &reply, &fm_error);

        mongoc_find_and_modify_opts_destroy(opts);
        bson_destroy(&query);
        bson_destroy(&update);
        bson_destroy(&sort);

        if (!ok) {
            bson_destroy(&reply);
            usleep(500 * 1000);
            continue;
        }

        bson_iter_t it;
        if (!bson_iter_init_find(&it, &reply, "value") || !BSON_ITER_HOLDS_DOCUMENT(&it)) {
            bson_destroy(&reply);
            usleep(500 * 1000);
            continue;
        }

        const uint8_t* data = NULL;
        uint32_t data_len = 0;
        bson_iter_document(&it, &data_len, &data);
        bson_t value;
        bson_init_static(&value, data, data_len);

        const char* url = NULL;
        const char* source = NULL;
        int depth = 0;

        if (bson_iter_init_find(&it, &value, "url") && BSON_ITER_HOLDS_UTF8(&it)) {
            url = bson_iter_utf8(&it, NULL);
        }
        if (bson_iter_init_find(&it, &value, "source") && BSON_ITER_HOLDS_UTF8(&it)) {
            source = bson_iter_utf8(&it, NULL);
        }
        if (bson_iter_init_find(&it, &value, "depth") && BSON_ITER_HOLDS_INT32(&it)) {
            depth = bson_iter_int32(&it);
        }

        if (!url) {
            bson_destroy(&reply);
            usleep(500 * 1000);
            continue;
        }

    if (!url_allowed(url, &cfg)) {
            bson_t del_q;
            bson_init(&del_q);
            BSON_APPEND_UTF8(&del_q, "url", url);
            mongoc_collection_delete_one(queue, &del_q, NULL, NULL, NULL);
            bson_destroy(&del_q);
            bson_destroy(&reply);
            continue;
        }

        HttpResult res = fetch_url(url, &cfg);
        if (!res.ok || res.status < 200 || res.status >= 400) {
            // Drop permanently if fetch failed to avoid infinite retry loop
            bson_t err_q;
            bson_init(&err_q);
            BSON_APPEND_UTF8(&err_q, "url", url);
            mongoc_collection_delete_one(queue, &err_q, NULL, NULL, NULL);
            bson_destroy(&err_q);
            bson_destroy(&reply);
            usleep(cfg.delay_ms * 1000);
            continue;
        }

        const char* effective_url = res.final_url ? res.final_url : url;
        char* normalized = normalize_url(effective_url);
        char* hash = sha256_hex(res.body, strlen(res.body));

        if (url_store_allowed(normalized, &cfg)) {
            bson_t existing;
            int has_existing = find_one_page(pages, normalized, &existing);
            if (has_existing) {
                const char* old_hash = NULL;
                if (bson_iter_init_find(&it, &existing, "hash") && BSON_ITER_HOLDS_UTF8(&it)) {
                    old_hash = bson_iter_utf8(&it, NULL);
                }
                if (!old_hash || strcmp(old_hash, hash) != 0) {
                    update_or_insert_page(pages, normalized, res.body, source ? source : "", hash);
                } else {
                    update_last_checked(pages, normalized);
                }
                bson_destroy(&existing);
            } else {
                update_or_insert_page(pages, normalized, res.body, source ? source : "", hash);
            }
        }

        if (cfg.follow_links && depth < cfg.max_depth) {
            StrList links;
            strlist_init(&links);
            extract_links(res.body, cfg.max_links_per_page, &links);
            if (links.count > 0) {
                StrList filtered;
                strlist_init(&filtered);
                for (int i = 0; i < links.count; ++i) {
                    if (url_allowed(links.items[i], &cfg)) {
                        strlist_add(&filtered, links.items[i]);
                    }
                }
                enqueue_links(queue, &filtered, source ? source : "", depth + 1, &cfg);
                strlist_free(&filtered);
            }
            strlist_free(&links);
        }

        bson_t del_q;
        bson_init(&del_q);
        BSON_APPEND_UTF8(&del_q, "url", url);
        mongoc_collection_delete_one(queue, &del_q, NULL, NULL, NULL);
        bson_destroy(&del_q);

        bson_destroy(&reply);
        free(res.body);
        free(res.final_url);
        free(normalized);
        free(hash);

        usleep(cfg.delay_ms * 1000);
    }

    mongoc_collection_destroy(pages);
    mongoc_collection_destroy(queue);
    mongoc_client_destroy(client);
    mongoc_cleanup();
    config_free(&cfg);

    printf("Stopped.\n");
    return 0;
}
