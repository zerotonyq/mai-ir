#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#ifndef _WIN32
#include <sys/time.h>
#endif

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

typedef struct Entry {
    char *key;
    uint64_t count;
    struct Entry *next;
} Entry;

typedef struct {
    Entry **buckets;
    size_t bucket_count;
    size_t size;
} HashTable;

typedef struct {
    uint64_t total_tokens;
    uint64_t total_len;
    uint64_t total_bytes;
} Stats;

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
    ht->buckets = (Entry **)calloc(bucket_count, sizeof(Entry *));
}

static void ht_free(HashTable *ht) {
    if (!ht || !ht->buckets) return;
    for (size_t i = 0; i < ht->bucket_count; ++i) {
        Entry *e = ht->buckets[i];
        while (e) {
            Entry *next = e->next;
            free(e->key);
            free(e);
            e = next;
        }
    }
    free(ht->buckets);
    ht->buckets = NULL;
    ht->bucket_count = 0;
    ht->size = 0;
}

static Entry *ht_get_or_add(HashTable *ht, const char *key) {
    uint64_t h = fnv1a_hash(key);
    size_t idx = (size_t)(h % ht->bucket_count);
    Entry *e = ht->buckets[idx];
    while (e) {
        if (strcmp(e->key, key) == 0) return e;
        e = e->next;
    }
    Entry *ne = (Entry *)calloc(1, sizeof(Entry));
    if (!ne) return NULL;
    size_t len = strlen(key);
    ne->key = (char *)malloc(len + 1);
    if (!ne->key) {
        free(ne);
        return NULL;
    }
    memcpy(ne->key, key, len + 1);
    ne->count = 0;
    ne->next = ht->buckets[idx];
    ht->buckets[idx] = ne;
    ht->size++;
    return ne;
}

static int is_ascii_token_char(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}

static int is_utf8_cyrillic_start(unsigned char c) {
    return c == 0xD0 || c == 0xD1;
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
            if (*p == 0xD0 && p[1] == 0x81) { // ?
                *p = 0xD1; p[1] = 0x91; // ?
            } else if (*p == 0xD0 && p[1] >= 0x90 && p[1] <= 0x9F) { // ?-?
                p[1] = (unsigned char)(p[1] + 0x20);
            } else if (*p == 0xD0 && p[1] >= 0xA0 && p[1] <= 0xAF) { // ?-?
                *p = 0xD1; p[1] = (unsigned char)(p[1] - 0x20);
            }
            p += 2;
            continue;
        }
        // skip other UTF-8 sequences
        if ((*p & 0xE0) == 0xC0 && p[1]) {
            p += 2;
        } else if ((*p & 0xF0) == 0xE0 && p[1] && p[2]) {
            p += 3;
        } else if ((*p & 0xF8) == 0xF0 && p[1] && p[2] && p[3]) {
            p += 4;
        } else {
            p++;
        }
    }
}

static double now_seconds(void) {
#ifdef _WIN32
    static LARGE_INTEGER freq;
    static int freq_init = 0;
    if (!freq_init) {
        QueryPerformanceFrequency(&freq);
        freq_init = 1;
    }
    LARGE_INTEGER counter;
    QueryPerformanceCounter(&counter);
    return (double)counter.QuadPart / (double)freq.QuadPart;
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1e6;
#endif
}

static int is_internal_joiner(unsigned char c) {
    return c == '-' || c == '\'';
}

typedef struct {
    int strip_html;
    int fit_mandelbrot;
    const char *out_csv;
} Options;

static int process_file(FILE *f, const Options *opt, HashTable *ht, Stats *stats) {
    const size_t BUFSZ = 64 * 1024;
    unsigned char *buf = (unsigned char *)malloc(BUFSZ);
    if (!buf) return 0;

    StrBuf token;
    sb_init(&token);
    int in_tag = 0;

    size_t carry = 0;
    while (1) {
        size_t n = fread(buf + carry, 1, BUFSZ - carry, f);
        if (n == 0 && carry == 0) break;
        size_t total = n + carry;
        stats->total_bytes += n;

        carry = 0;
        if (total > 0) {
            unsigned char last = buf[total - 1];
            if (last == 0xD0 || last == 0xD1) {
                carry = 1;
            }
        }
        size_t total_process = total - carry;
        size_t i = 0;
        while (i < total_process) {
            unsigned char c = buf[i];

            if (opt->strip_html) {
                if (in_tag) {
                    if (c == '>') in_tag = 0;
                    i++;
                    continue;
                } else if (c == '<') {
                    in_tag = 1;
                    i++;
                    continue;
                }
            }

            if (is_ascii_token_char(c)) {
                if (!sb_push(&token, (const char *)&c, 1)) goto fail;
                i++;
                continue;
            }

            int cylen = utf8_cyrillic_len(buf + i, total - i);
            if (cylen == 2) {
                if (!sb_push(&token, (const char *)(buf + i), 2)) goto fail;
                i += 2;
                continue;
            }

            if (is_internal_joiner(c)) {
                // allow joiner only if surrounded by token chars
                unsigned char next = 0;
                int has_next = 0;
                if (i + 1 < total) {
                    next = buf[i + 1];
                    has_next = 1;
                }
                int prev_ok = token.len > 0;
                int next_ok = 0;
                if (has_next) {
                    if (is_ascii_token_char(next)) next_ok = 1;
                    else if (utf8_cyrillic_len(buf + i + 1, total - (i + 1)) == 2) next_ok = 1;
                }
                if (prev_ok && next_ok) {
                    if (!sb_push(&token, (const char *)&c, 1)) goto fail;
                    i++;
                    continue;
                }
            }

            if (token.len > 0) {
                lowercase_utf8_inplace(token.data);
                Entry *e = ht_get_or_add(ht, token.data);
                if (!e) goto fail;
                e->count++;
                stats->total_tokens++;
                stats->total_len += token.len;
                sb_clear(&token);
            }
            i++;
        }

        if (n == 0) break;

        if (carry > 0) {
            memmove(buf, buf + total_process, carry);
        }
    }

    if (token.len > 0) {
        lowercase_utf8_inplace(token.data);
        Entry *e = ht_get_or_add(ht, token.data);
        if (!e) goto fail;
        e->count++;
        stats->total_tokens++;
        stats->total_len += token.len;
    }

    sb_free(&token);
    free(buf);
    return 1;

fail:
    sb_free(&token);
    free(buf);
    return 0;
}

static int process_path(const char *path, const Options *opt, HashTable *ht, Stats *stats) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "Failed to open %s\n", path);
        return 0;
    }
    int ok = process_file(f, opt, ht, stats);
    fclose(f);
    return ok;
}

static int process_list(const char *list_path, const Options *opt, HashTable *ht, Stats *stats) {
    FILE *f = fopen(list_path, "rb");
    if (!f) {
        fprintf(stderr, "Failed to open list %s\n", list_path);
        return 0;
    }
    char line[4096];
    int ok = 1;
    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) continue;
        if (!process_path(line, opt, ht, stats)) {
            ok = 0;
            break;
        }
    }
    fclose(f);
    return ok;
}

typedef struct {
    Entry **items;
    size_t count;
} EntryList;

static void entrylist_from_ht(const HashTable *ht, EntryList *out) {
    out->count = ht->size;
    out->items = (Entry **)malloc(sizeof(Entry *) * out->count);
    size_t idx = 0;
    for (size_t i = 0; i < ht->bucket_count; ++i) {
        Entry *e = ht->buckets[i];
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

static void swap_entries(Entry **a, Entry **b) {
    Entry *t = *a;
    *a = *b;
    *b = t;
}

static int entry_cmp(const Entry *a, const Entry *b) {
    if (a->count > b->count) return -1;
    if (a->count < b->count) return 1;
    return strcmp(a->key, b->key);
}

static void quicksort_entries(Entry **arr, long left, long right) {
    if (left >= right) return;
    long i = left;
    long j = right;
    Entry *pivot = arr[(left + right) / 2];
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

static double fit_mandelbrot(const EntryList *list, double *out_alpha, double *out_b, double *out_c) {
    // brute-force grid search in log-space
    double best_err = 1e100;
    double best_a = 1.0, best_b = 0.0, best_c = 1.0;
    size_t n = list->count;
    if (n == 0) {
        *out_alpha = best_a;
        *out_b = best_b;
        *out_c = best_c;
        return best_err;
    }

    for (double a = 0.8; a <= 1.2; a += 0.02) {
        for (double b = 0.0; b <= 10.0; b += 0.5) {
            double sum_y = 0.0;
            double sum_x = 0.0;
            double sum_x2 = 0.0;
            double sum_xy = 0.0;
            size_t m = 0;
            for (size_t i = 0; i < n; ++i) {
                double r = (double)(i + 1);
                double x = -a * log(r + b);
                double y = log((double)list->items[i]->count);
                sum_x += x;
                sum_y += y;
                sum_x2 += x * x;
                sum_xy += x * y;
                m++;
            }
            double denom = (double)m * sum_x2 - sum_x * sum_x;
            if (denom == 0.0) continue;
            double intercept = (sum_y * sum_x2 - sum_x * sum_xy) / denom;
            double c = exp(intercept);

            double err = 0.0;
            for (size_t i = 0; i < n; ++i) {
                double r = (double)(i + 1);
                double pred = c / pow(r + b, a);
                double y = log((double)list->items[i]->count);
                double yhat = log(pred);
                double d = y - yhat;
                err += d * d;
            }
            if (err < best_err) {
                best_err = err;
                best_a = a;
                best_b = b;
                best_c = c;
            }
        }
    }

    *out_alpha = best_a;
    *out_b = best_b;
    *out_c = best_c;
    return best_err;
}

static void print_usage(const char *prog) {
    printf("Usage: %s --input <file> | --list <list.txt> [--out freq.csv] [--strip-html] [--fit-mandelbrot]\n", prog);
}

int main(int argc, char **argv) {
    Options opt;
    opt.strip_html = 0;
    opt.fit_mandelbrot = 0;
    opt.out_csv = "freq.csv";

    const char *input_path = NULL;
    const char *list_path = NULL;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            input_path = argv[++i];
        } else if (strcmp(argv[i], "--list") == 0 && i + 1 < argc) {
            list_path = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            opt.out_csv = argv[++i];
        } else if (strcmp(argv[i], "--strip-html") == 0) {
            opt.strip_html = 1;
        } else if (strcmp(argv[i], "--fit-mandelbrot") == 0) {
            opt.fit_mandelbrot = 1;
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    if (!input_path && !list_path) {
        print_usage(argv[0]);
        return 1;
    }

    HashTable ht;
    ht_init(&ht, 100003);
    Stats stats = {0, 0, 0};

    double t0 = now_seconds();
    int ok = 1;
    if (input_path) ok = process_path(input_path, &opt, &ht, &stats);
    if (ok && list_path) ok = process_list(list_path, &opt, &ht, &stats);
    double t1 = now_seconds();

    if (!ok) {
        ht_free(&ht);
        return 1;
    }

    EntryList list;
    entrylist_from_ht(&ht, &list);
    if (list.count > 0) {
        quicksort_entries(list.items, 0, (long)list.count - 1);
    }

    double elapsed = t1 - t0;
    double kb = (double)stats.total_bytes / 1024.0;
    double speed = elapsed > 0.0 ? kb / elapsed : 0.0;
    double avg_len = stats.total_tokens ? (double)stats.total_len / (double)stats.total_tokens : 0.0;

    printf("Bytes: %llu\n", (unsigned long long)stats.total_bytes);
    printf("Tokens: %llu\n", (unsigned long long)stats.total_tokens);
    printf("AvgTokenLen: %.3f\n", avg_len);
    printf("UniqueTerms: %zu\n", list.count);
    printf("TimeSeconds: %.6f\n", elapsed);
    printf("SpeedKBps: %.3f\n", speed);

    double alpha = 0.0, b = 0.0, c = 0.0;
    if (opt.fit_mandelbrot) {
        fit_mandelbrot(&list, &alpha, &b, &c);
        printf("Mandelbrot: alpha=%.3f b=%.3f c=%.3f\n", alpha, b, c);
    }

    FILE *out = fopen(opt.out_csv, "wb");
    if (!out) {
        fprintf(stderr, "Failed to open output %s\n", opt.out_csv);
        entrylist_free(&list);
        ht_free(&ht);
        return 1;
    }
    fprintf(out, "rank,freq,zipf,mandelbrot\n");
    uint64_t f1 = list.count > 0 ? list.items[0]->count : 0;
    for (size_t i = 0; i < list.count; ++i) {
        double r = (double)(i + 1);
        double zipf = (r > 0.0) ? (double)f1 / r : 0.0;
        double mand = 0.0;
        if (opt.fit_mandelbrot) {
            mand = c / pow(r + b, alpha);
        }
        fprintf(out, "%zu,%llu,%.6f,%.6f\n", i + 1,
                (unsigned long long)list.items[i]->count,
                zipf, mand);
    }
    fclose(out);

    entrylist_free(&list);
    ht_free(&ht);
    return 0;
}
