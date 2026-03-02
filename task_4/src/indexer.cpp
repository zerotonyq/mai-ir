#include <bson/bson.h>
#include <mongoc/mongoc.h>

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/time.h>
#endif

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} StrBuf;

static void sb_init(StrBuf *sb) {
    sb->data = NULL;
    sb->len = 0;
    sb->cap = 0;
}

static int sb_reserve(StrBuf *sb, size_t need) {
    if (need <= sb->cap) return 1;
    size_t new_cap = sb->cap == 0 ? 64 : sb->cap * 2;
    while (new_cap < need) new_cap *= 2;
    char *next = (char *)realloc(sb->data, new_cap);
    if (!next) return 0;
    sb->data = next;
    sb->cap = new_cap;
    return 1;
}

static int sb_push(StrBuf *sb, const char *bytes, size_t n) {
    if (!sb_reserve(sb, sb->len + n + 1)) return 0;
    memcpy(sb->data + sb->len, bytes, n);
    sb->len += n;
    sb->data[sb->len] = '\0';
    return 1;
}

static void sb_clear(StrBuf *sb) {
    sb->len = 0;
    if (sb->data) sb->data[0] = '\0';
}

static void sb_free(StrBuf *sb) {
    free(sb->data);
    sb->data = NULL;
    sb->len = 0;
    sb->cap = 0;
}

static double now_seconds(void) {
#ifdef _WIN32
    return 0.0;
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1e6;
#endif
}

static int is_ascii_token_char(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}

static int utf8_cyrillic_len(const unsigned char *p, size_t n) {
    if (n < 2) return 0;
    if ((p[0] == 0xD0 || p[0] == 0xD1) && (p[1] >= 0x80 && p[1] <= 0xBF)) return 2;
    return 0;
}

static void lowercase_utf8_inplace(char *s) {
    unsigned char *p = (unsigned char *)s;
    while (*p) {
        if (*p < 0x80) {
            *p = (unsigned char)tolower(*p);
            p++;
            continue;
        }
        if ((*p == 0xD0 || *p == 0xD1) && p[1]) {
            if (*p == 0xD0 && p[1] == 0x81) {
                *p = 0xD1; p[1] = 0x91;
            } else if (*p == 0xD0 && p[1] >= 0x90 && p[1] <= 0x9F) {
                p[1] = (unsigned char)(p[1] + 0x20);
            } else if (*p == 0xD0 && p[1] >= 0xA0 && p[1] <= 0xAF) {
                *p = 0xD1; p[1] = (unsigned char)(p[1] - 0x20);
            }
            p += 2;
            continue;
        }
        if ((*p & 0xE0) == 0xC0 && p[1]) p += 2;
        else if ((*p & 0xF0) == 0xE0 && p[1] && p[2]) p += 3;
        else if ((*p & 0xF8) == 0xF0 && p[1] && p[2] && p[3]) p += 4;
        else p++;
    }
}

static int is_internal_joiner(unsigned char c) {
    return c == '-' || c == '\'';
}

static int contains_cyrillic(const char *s) {
    const unsigned char *p = (const unsigned char *)s;
    while (*p) {
        if (*p == 0xD0 || *p == 0xD1) return 1;
        p++;
    }
    return 0;
}

static int ends_with(const char *s, const char *suf) {
    size_t sl = strlen(s);
    size_t sufl = strlen(suf);
    if (sufl > sl) return 0;
    return memcmp(s + sl - sufl, suf, sufl) == 0;
}

static void strip_suffix(char *s, const char *suf) {
    size_t sl = strlen(s);
    size_t sufl = strlen(suf);
    if (sufl <= sl) s[sl - sufl] = '\0';
}

static void stem_ru(char *s) {
    if (!contains_cyrillic(s)) return;
    const char *suffixes[] = {
        "\xD1\x8F\xD0\xBC\xD0\xB8", "\xD0\xB0\xD0\xBC\xD0\xB8", "\xD0\xBE\xD0\xB3\xD0\xBE",
        "\xD0\xB5\xD0\xBC\xD1\x83", "\xD0\xBE\xD0\xBC\xD1\x83", "\xD0\xB5\xD0\xB5",
        "\xD0\xB8\xD0\xB5", "\xD1\x8B\xD0\xB5", "\xD0\xB8\xD0\xB9",
        "\xD1\x8B\xD0\xB9", "\xD0\xBE\xD0\xB9", "\xD0\xB0\xD1\x8F", "\xD1\x8F\xD1\x8F",
        "\xD0\xB0\xD0\xBC", "\xD1\x8F\xD0\xBC", "\xD0\xB0\xD1\x85", "\xD1\x8F\xD1\x85",
        "\xD0\xBE\xD0\xB2", "\xD0\xB5\xD0\xB2", "\xD0\xB5\xD0\xB9",
        "\xD0\xBE\xD0\xBC", "\xD0\xB5\xD0\xBC", "\xD1\x8B\xD0\xBC", "\xD0\xB8\xD0\xBC",
        "\xD0\xB0", "\xD1\x8F", "\xD0\xBE", "\xD0\xB5", "\xD0\xB8", "\xD1\x8B", "\xD1\x83", "\xD1\x8E"
    };
    size_t count = sizeof(suffixes) / sizeof(suffixes[0]);
    for (size_t i = 0; i < count; ++i) {
        if (ends_with(s, suffixes[i])) {
            strip_suffix(s, suffixes[i]);
            break;
        }
    }
}

typedef struct PostingList {
    int *items;
    int count;
    int cap;
} PostingList;

typedef struct TermEntry {
    char *term;
    uint64_t tf;
    int last_doc_id;
    PostingList postings;
    struct TermEntry *next;
} TermEntry;

typedef struct {
    TermEntry **buckets;
    size_t bucket_count;
    size_t size;
} HashTable;

static uint64_t fnv1a_hash(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ULL;
    }
    return h;
}

static void ht_init(HashTable *ht, size_t bucket_count) {
    ht->bucket_count = bucket_count;
    ht->size = 0;
    ht->buckets = (TermEntry **)calloc(bucket_count, sizeof(TermEntry *));
}

static void ht_free(HashTable *ht) {
    if (!ht || !ht->buckets) return;
    for (size_t i = 0; i < ht->bucket_count; ++i) {
        TermEntry *e = ht->buckets[i];
        while (e) {
            TermEntry *next = e->next;
            free(e->term);
            free(e->postings.items);
            free(e);
            e = next;
        }
    }
    free(ht->buckets);
    ht->buckets = NULL;
    ht->bucket_count = 0;
    ht->size = 0;
}

static TermEntry *ht_get_or_add(HashTable *ht, const char *key) {
    uint64_t h = fnv1a_hash(key);
    size_t idx = (size_t)(h % ht->bucket_count);
    TermEntry *e = ht->buckets[idx];
    while (e) {
        if (strcmp(e->term, key) == 0) return e;
        e = e->next;
    }
    TermEntry *ne = (TermEntry *)calloc(1, sizeof(TermEntry));
    if (!ne) return NULL;
    size_t len = strlen(key);
    ne->term = (char *)malloc(len + 1);
    if (!ne->term) { free(ne); return NULL; }
    memcpy(ne->term, key, len + 1);
    ne->tf = 0;
    ne->last_doc_id = -1;
    ne->postings.items = NULL;
    ne->postings.count = 0;
    ne->postings.cap = 0;
    ne->next = ht->buckets[idx];
    ht->buckets[idx] = ne;
    ht->size++;
    return ne;
}

static int postings_add(PostingList *pl, int doc_id) {
    if (pl->count == pl->cap) {
        int new_cap = pl->cap == 0 ? 8 : pl->cap * 2;
        int *next = (int *)realloc(pl->items, sizeof(int) * new_cap);
        if (!next) return 0;
        pl->items = next;
        pl->cap = new_cap;
    }
    pl->items[pl->count++] = doc_id;
    return 1;
}

typedef struct {
    int id;
    char *url;
    char *title;
} Doc;

typedef struct {
    Doc *items;
    int count;
    int cap;
} DocList;

static void doclist_init(DocList *dl) {
    dl->items = NULL;
    dl->count = 0;
    dl->cap = 0;
}

static void doclist_free(DocList *dl) {
    for (int i = 0; i < dl->count; ++i) {
        free(dl->items[i].url);
        free(dl->items[i].title);
    }
    free(dl->items);
    dl->items = NULL;
    dl->count = 0;
    dl->cap = 0;
}

static Doc *doclist_add(DocList *dl) {
    if (dl->count == dl->cap) {
        int new_cap = dl->cap == 0 ? 256 : dl->cap * 2;
        Doc *next = (Doc *)realloc(dl->items, sizeof(Doc) * new_cap);
        if (!next) return NULL;
        dl->items = next;
        dl->cap = new_cap;
    }
    Doc *d = &dl->items[dl->count++];
    d->id = dl->count - 1;
    d->url = NULL;
    d->title = NULL;
    return d;
}

typedef struct {
    uint64_t total_tokens;
    uint64_t total_len;
    uint64_t input_bytes;
} Stats;

static int term_add(HashTable *ht, const char *term, int doc_id, Stats *stats) {
    TermEntry *e = ht_get_or_add(ht, term);
    if (!e) return 0;
    e->tf++;
    stats->total_tokens++;
    stats->total_len += strlen(term);
    if (e->last_doc_id != doc_id) {
        if (!postings_add(&e->postings, doc_id)) return 0;
        e->last_doc_id = doc_id;
    }
    return 1;
}

static int is_tag_start(unsigned char c) { return c == '<'; }
static int is_tag_end(unsigned char c) { return c == '>'; }

static int tokenize_text(const char *text, size_t len, int doc_id, HashTable *ht, Stats *stats) {
    StrBuf token;
    sb_init(&token);

    size_t i = 0;
    while (i < len) {
        unsigned char c = (unsigned char)text[i];
        stats->input_bytes++;

        if (is_ascii_token_char(c)) {
            if (!sb_push(&token, (const char *)&c, 1)) { sb_free(&token); return 0; }
            i++;
            continue;
        }

        int cylen = utf8_cyrillic_len((const unsigned char *)text + i, len - i);
        if (cylen == 2) {
            if (!sb_push(&token, text + i, 2)) { sb_free(&token); return 0; }
            i += 2;
            continue;
        }

        if (is_internal_joiner(c)) {
            unsigned char next = 0;
            int has_next = 0;
            if (i + 1 < len) { next = (unsigned char)text[i + 1]; has_next = 1; }
            int prev_ok = token.len > 0;
            int next_ok = 0;
            if (has_next) {
                if (is_ascii_token_char(next)) next_ok = 1;
                else if (utf8_cyrillic_len((const unsigned char *)text + i + 1, len - (i + 1)) == 2) next_ok = 1;
            }
            if (prev_ok && next_ok) {
                if (!sb_push(&token, (const char *)&c, 1)) { sb_free(&token); return 0; }
                i++;
                continue;
            }
        }

        if (token.len > 0) {
            lowercase_utf8_inplace(token.data);
            stem_ru(token.data);
            if (token.len > 0) {
                if (!term_add(ht, token.data, doc_id, stats)) { sb_free(&token); return 0; }
            }
            sb_clear(&token);
        }
        i++;
    }

    if (token.len > 0) {
        lowercase_utf8_inplace(token.data);
        stem_ru(token.data);
        if (token.len > 0) {
            if (!term_add(ht, token.data, doc_id, stats)) { sb_free(&token); return 0; }
        }
    }

    sb_free(&token);
    return 1;
}

static int tokenize_html(const char *html, size_t len, int doc_id, HashTable *ht, Stats *stats) {
    StrBuf token;
    sb_init(&token);
    int in_tag = 0;

    size_t i = 0;
    while (i < len) {
        unsigned char c = (unsigned char)html[i];
        stats->input_bytes++;

        if (in_tag) {
            if (is_tag_end(c)) in_tag = 0;
            i++;
            continue;
        }
        if (is_tag_start(c)) {
            in_tag = 1;
            i++;
            continue;
        }

        if (is_ascii_token_char(c)) {
            if (!sb_push(&token, (const char *)&c, 1)) { sb_free(&token); return 0; }
            i++;
            continue;
        }

        int cylen = utf8_cyrillic_len((const unsigned char *)html + i, len - i);
        if (cylen == 2) {
            if (!sb_push(&token, html + i, 2)) { sb_free(&token); return 0; }
            i += 2;
            continue;
        }

        if (is_internal_joiner(c)) {
            unsigned char next = 0;
            int has_next = 0;
            if (i + 1 < len) { next = (unsigned char)html[i + 1]; has_next = 1; }
            int prev_ok = token.len > 0;
            int next_ok = 0;
            if (has_next) {
                if (is_ascii_token_char(next)) next_ok = 1;
                else if (utf8_cyrillic_len((const unsigned char *)html + i + 1, len - (i + 1)) == 2) next_ok = 1;
            }
            if (prev_ok && next_ok) {
                if (!sb_push(&token, (const char *)&c, 1)) { sb_free(&token); return 0; }
                i++;
                continue;
            }
        }

        if (token.len > 0) {
            lowercase_utf8_inplace(token.data);
            stem_ru(token.data);
            if (token.len > 0) {
                if (!term_add(ht, token.data, doc_id, stats)) { sb_free(&token); return 0; }
            }
            sb_clear(&token);
        }
        i++;
    }

    if (token.len > 0) {
        lowercase_utf8_inplace(token.data);
        stem_ru(token.data);
        if (token.len > 0) {
            if (!term_add(ht, token.data, doc_id, stats)) { sb_free(&token); return 0; }
        }
    }

    sb_free(&token);
    return 1;
}

typedef struct {
    TermEntry **items;
    size_t count;
} EntryList;

static void entrylist_from_ht(const HashTable *ht, EntryList *out) {
    out->count = ht->size;
    out->items = (TermEntry **)malloc(sizeof(TermEntry *) * out->count);
    size_t idx = 0;
    for (size_t i = 0; i < ht->bucket_count; ++i) {
        TermEntry *e = ht->buckets[i];
        while (e) {
            out->items[idx++] = e;
            e = e->next;
        }
    }
}

static void entrylist_free(EntryList *list) {
    free(list->items);
    list->items = NULL;
    list->count = 0;
}

static void swap_entries(TermEntry **a, TermEntry **b) {
    TermEntry *t = *a; *a = *b; *b = t;
}

static int entry_cmp(const TermEntry *a, const TermEntry *b) {
    if (a->tf > b->tf) return -1;
    if (a->tf < b->tf) return 1;
    return strcmp(a->term, b->term);
}

static void quicksort_entries(TermEntry **arr, long left, long right) {
    if (left >= right) return;
    long i = left, j = right;
    TermEntry *pivot = arr[(left + right) / 2];
    while (i <= j) {
        while (entry_cmp(arr[i], pivot) < 0) i++;
        while (entry_cmp(arr[j], pivot) > 0) j--;
        if (i <= j) {
            swap_entries(&arr[i], &arr[j]);
            i++; j--;
        }
    }
    if (left < j) quicksort_entries(arr, left, j);
    if (i < right) quicksort_entries(arr, i, right);
}

static void print_usage(const char *prog) {
    printf("Usage: %s --mongo-uri <uri> --db <db> --collection <coll> --out <dir> [--limit N]\n", prog);
}

int main(int argc, char **argv) {
    const char *mongo_uri = NULL;
    const char *db = NULL;
    const char *coll = NULL;
    const char *out_dir = NULL;
    int limit = 0;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--mongo-uri") == 0 && i + 1 < argc) mongo_uri = argv[++i];
        else if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) db = argv[++i];
        else if (strcmp(argv[i], "--collection") == 0 && i + 1 < argc) coll = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_dir = argv[++i];
        else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) limit = atoi(argv[++i]);
        else { print_usage(argv[0]); return 1; }
    }

    if (!mongo_uri || !db || !coll || !out_dir) {
        print_usage(argv[0]);
        return 1;
    }

    mongoc_init();
    mongoc_client_t *client = mongoc_client_new(mongo_uri);
    if (!client) {
        fprintf(stderr, "Failed to connect to MongoDB\n");
        return 1;
    }
    mongoc_collection_t *collection = mongoc_client_get_collection(client, db, coll);

    HashTable ht;
    ht_init(&ht, 200003);
    DocList docs;
    doclist_init(&docs);
    Stats stats = {0, 0, 0};

    bson_t query;
    bson_init(&query);

    bson_t opts;
    bson_init(&opts);
    if (limit > 0) BSON_APPEND_INT64(&opts, "limit", limit);

    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(collection, &query, &opts, NULL);
    const bson_t *doc;

    double t0 = now_seconds();

    while (mongoc_cursor_next(cursor, &doc)) {
        bson_iter_t it;
        const char *url = NULL;
        const char *text = NULL;
        const uint8_t *html = NULL;
        uint32_t html_len = 0;
        uint32_t text_len = 0;

        if (bson_iter_init_find(&it, doc, "url") && BSON_ITER_HOLDS_UTF8(&it)) {
            url = bson_iter_utf8(&it, NULL);
        }
        if (bson_iter_init_find(&it, doc, "text") && BSON_ITER_HOLDS_UTF8(&it)) {
            text = bson_iter_utf8(&it, &text_len);
        }
        if (!text || text_len == 0) {
            if (bson_iter_init_find(&it, doc, "html") && BSON_ITER_HOLDS_BINARY(&it)) {
                bson_subtype_t subtype;
                bson_iter_binary(&it, &subtype, &html_len, &html);
            }
        }
        if (!url) continue;
        if ((!text || text_len == 0) && (!html || html_len == 0)) continue;

        Doc *d = doclist_add(&docs);
        if (!d) break;
        d->url = strdup(url);
        d->title = NULL;

        int ok = 1;
        if (text && text_len > 0) {
            ok = tokenize_text(text, text_len, d->id, &ht, &stats);
        } else {
            ok = tokenize_html((const char *)html, html_len, d->id, &ht, &stats);
        }
        if (!ok) {
            fprintf(stderr, "Tokenization failed\n");
            break;
        }
    }

    double t1 = now_seconds();

    mongoc_cursor_destroy(cursor);
    bson_destroy(&query);
    bson_destroy(&opts);

    char path_docs[512];
    char path_index[512];
    char path_freq[512];
    char path_stats[512];

    snprintf(path_docs, sizeof(path_docs), "%s/docs.tsv", out_dir);
    snprintf(path_index, sizeof(path_index), "%s/index.tsv", out_dir);
    snprintf(path_freq, sizeof(path_freq), "%s/freq.csv", out_dir);
    snprintf(path_stats, sizeof(path_stats), "%s/stats.txt", out_dir);

    FILE *f_docs = fopen(path_docs, "wb");
    FILE *f_index = fopen(path_index, "wb");
    FILE *f_freq = fopen(path_freq, "wb");
    FILE *f_stats = fopen(path_stats, "wb");

    if (!f_docs || !f_index || !f_freq || !f_stats) {
        fprintf(stderr, "Failed to open output files in %s\n", out_dir);
        return 1;
    }

    for (int i = 0; i < docs.count; ++i) {
        fprintf(f_docs, "%d\t%s\n", docs.items[i].id, docs.items[i].url ? docs.items[i].url : "");
    }

    EntryList list;
    entrylist_from_ht(&ht, &list);
    if (list.count > 0) quicksort_entries(list.items, 0, (long)list.count - 1);

    fprintf(f_freq, "rank,freq,zipf\n");
    uint64_t f1 = list.count > 0 ? list.items[0]->tf : 0;
    for (size_t i = 0; i < list.count; ++i) {
        TermEntry *e = list.items[i];
        double r = (double)(i + 1);
        double zipf = f1 ? (double)f1 / r : 0.0;
        fprintf(f_freq, "%zu,%llu,%.6f\n", i + 1, (unsigned long long)e->tf, zipf);
    }

    for (size_t i = 0; i < list.count; ++i) {
        TermEntry *e = list.items[i];
        fprintf(f_index, "%s\t%d\t", e->term, e->postings.count);
        for (int j = 0; j < e->postings.count; ++j) {
            fprintf(f_index, "%d", e->postings.items[j]);
            if (j + 1 < e->postings.count) fputc(',', f_index);
        }
        fputc('\n', f_index);
    }

    double elapsed = t1 - t0;
    double kb = (double)stats.input_bytes / 1024.0;
    double speed = elapsed > 0.0 ? kb / elapsed : 0.0;
    double avg_len = stats.total_tokens ? (double)stats.total_len / (double)stats.total_tokens : 0.0;

    fprintf(f_stats, "Docs: %d\n", docs.count);
    fprintf(f_stats, "Terms: %zu\n", list.count);
    fprintf(f_stats, "Tokens: %llu\n", (unsigned long long)stats.total_tokens);
    fprintf(f_stats, "AvgTokenLen: %.5f\n", avg_len);
    fprintf(f_stats, "InputKB: %.3f\n", kb);
    fprintf(f_stats, "Elapsed: %.5f\n", elapsed);
    fprintf(f_stats, "SpeedKBps: %.3f\n", speed);

    fclose(f_docs);
    fclose(f_index);
    fclose(f_freq);
    fclose(f_stats);

    entrylist_free(&list);
    doclist_free(&docs);
    ht_free(&ht);

    mongoc_collection_destroy(collection);
    mongoc_client_destroy(client);
    mongoc_cleanup();

    printf("Docs: %d\n", docs.count);
    printf("Terms: %zu\n", list.count);
    printf("Tokens: %llu\n", (unsigned long long)stats.total_tokens);
    printf("AvgTokenLen: %.5f\n", avg_len);
    printf("InputKB: %.3f\n", kb);
    printf("Elapsed: %.5f\n", elapsed);
    printf("SpeedKBps: %.3f\n", speed);

    return 0;
}
