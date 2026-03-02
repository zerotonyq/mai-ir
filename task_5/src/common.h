#pragma once

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <limits.h>

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} StrBuf;

static void sb_init(StrBuf *sb) { sb->data = NULL; sb->len = 0; sb->cap = 0; }
static int sb_reserve(StrBuf *sb, size_t need) {
    if (need <= sb->cap) return 1;
    size_t new_cap = sb->cap == 0 ? 64 : sb->cap * 2;
    while (new_cap < need) new_cap *= 2;
    char *next = (char *)realloc(sb->data, new_cap);
    if (!next) return 0;
    sb->data = next; sb->cap = new_cap; return 1;
}
static int sb_push(StrBuf *sb, const char *bytes, size_t n) {
    if (!sb_reserve(sb, sb->len + n + 1)) return 0;
    memcpy(sb->data + sb->len, bytes, n);
    sb->len += n;
    sb->data[sb->len] = '\0';
    return 1;
}
static void sb_clear(StrBuf *sb) { sb->len = 0; if (sb->data) sb->data[0] = '\0'; }
static void sb_free(StrBuf *sb) { free(sb->data); sb->data = NULL; sb->len = 0; sb->cap = 0; }

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

static int contains_cyrillic(const char *s) {
    const unsigned char *p = (const unsigned char *)s;
    while (*p) { if (*p == 0xD0 || *p == 0xD1) return 1; p++; }
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
        if (ends_with(s, suffixes[i])) { strip_suffix(s, suffixes[i]); break; }
    }
}

static int is_internal_joiner(unsigned char c) { return c == '-' || c == '\''; }

// --- Dynamic list ---
typedef struct {
    int *items;
    int count;
    int cap;
} IntList;

static void il_init(IntList *l) { l->items = NULL; l->count = 0; l->cap = 0; }
static void il_free(IntList *l) { free(l->items); l->items = NULL; l->count = 0; l->cap = 0; }
static int il_push(IntList *l, int v) {
    if (l->count == l->cap) {
        int new_cap = l->cap == 0 ? 8 : l->cap * 2;
        int *next = (int *)realloc(l->items, sizeof(int) * new_cap);
        if (!next) return 0;
        l->items = next; l->cap = new_cap;
    }
    l->items[l->count++] = v;
    return 1;
}

// --- Hash table ---
typedef struct TermNode {
    char *term;
    IntList postings;
    struct TermNode *next;
} TermNode;

typedef struct {
    TermNode **buckets;
    size_t bucket_count;
    size_t size;
} HashTable;

static uint64_t fnv1a_hash(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    while (*s) { h ^= (unsigned char)*s++; h *= 1099511628211ULL; }
    return h;
}

static void ht_init(HashTable *ht, size_t bucket_count) {
    ht->bucket_count = bucket_count; ht->size = 0;
    ht->buckets = (TermNode **)calloc(bucket_count, sizeof(TermNode *));
}

static void ht_free(HashTable *ht) {
    if (!ht || !ht->buckets) return;
    for (size_t i = 0; i < ht->bucket_count; ++i) {
        TermNode *e = ht->buckets[i];
        while (e) {
            TermNode *next = e->next;
            free(e->term); il_free(&e->postings); free(e);
            e = next;
        }
    }
    free(ht->buckets); ht->buckets = NULL; ht->bucket_count = 0; ht->size = 0;
}

static TermNode *ht_get(HashTable *ht, const char *key) {
    uint64_t h = fnv1a_hash(key);
    size_t idx = (size_t)(h % ht->bucket_count);
    TermNode *e = ht->buckets[idx];
    while (e) { if (strcmp(e->term, key) == 0) return e; e = e->next; }
    return NULL;
}

static TermNode *ht_get_or_add(HashTable *ht, const char *key) {
    TermNode *e = ht_get(ht, key);
    if (e) return e;
    uint64_t h = fnv1a_hash(key);
    size_t idx = (size_t)(h % ht->bucket_count);
    TermNode *ne = (TermNode *)calloc(1, sizeof(TermNode));
    if (!ne) return NULL;
    size_t len = strlen(key);
    ne->term = (char *)malloc(len + 1);
    if (!ne->term) { free(ne); return NULL; }
    memcpy(ne->term, key, len + 1);
    il_init(&ne->postings);
    ne->next = ht->buckets[idx];
    ht->buckets[idx] = ne;
    ht->size++;
    return ne;
}

// --- Docs ---
typedef struct { int id; char *url; } Doc;

typedef struct {
    Doc *items; int count; int cap;
} DocList;

static void doclist_init(DocList *dl) { dl->items = NULL; dl->count = 0; dl->cap = 0; }
static void doclist_free(DocList *dl) {
    for (int i = 0; i < dl->count; ++i) free(dl->items[i].url);
    free(dl->items); dl->items = NULL; dl->count = 0; dl->cap = 0;
}
static int doclist_add(DocList *dl, int id, const char *url) {
    if (dl->count == dl->cap) {
        int new_cap = dl->cap == 0 ? 256 : dl->cap * 2;
        Doc *next = (Doc *)realloc(dl->items, sizeof(Doc) * new_cap);
        if (!next) return 0;
        dl->items = next; dl->cap = new_cap;
    }
    Doc *d = &dl->items[dl->count++];
    d->id = id;
    d->url = strdup(url ? url : "");
    return 1;
}

// --- Index loading ---
static int load_docs(const char *path, DocList *docs) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    char line[8192];
    while (fgets(line, sizeof(line), f)) {
        char *tab = strchr(line, '\t');
        if (!tab) continue;
        *tab = '\0';
        int id = atoi(line);
        char *url = tab + 1;
        size_t len = strlen(url);
        while (len > 0 && (url[len - 1] == '\n' || url[len - 1] == '\r')) url[--len] = '\0';
        doclist_add(docs, id, url);
    }
    fclose(f);
    return 1;
}

static int load_index(const char *path, HashTable *ht) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    char line[1 << 20];
    while (fgets(line, sizeof(line), f)) {
        char *p = strchr(line, '\t');
        if (!p) continue;
        *p++ = '\0';
        char *p2 = strchr(p, '\t');
        if (!p2) continue;
        *p2++ = '\0';
        char *term = line;
        TermNode *node = ht_get_or_add(ht, term);
        if (!node) continue;
        char *list = p2;
        size_t len = strlen(list);
        while (len > 0 && (list[len - 1] == '\n' || list[len - 1] == '\r')) list[--len] = '\0';
        char *tok = list;
        while (tok && *tok) {
            char *comma = strchr(tok, ',');
            if (comma) *comma = '\0';
            int id = atoi(tok);
            il_push(&node->postings, id);
            tok = comma ? comma + 1 : NULL;
        }
    }
    fclose(f);
    return 1;
}

// --- Query parsing ---
typedef enum { TK_TERM, TK_AND, TK_OR, TK_NOT, TK_LPAREN, TK_RPAREN } TokenType;

typedef struct {
    TokenType type;
    char *text;
} Token;

typedef struct {
    Token *items; int count; int cap;
} TokenList;

static void tl_init(TokenList *tl) { tl->items = NULL; tl->count = 0; tl->cap = 0; }
static void tl_free(TokenList *tl) {
    for (int i = 0; i < tl->count; ++i) free(tl->items[i].text);
    free(tl->items); tl->items = NULL; tl->count = 0; tl->cap = 0;
}
static int tl_push(TokenList *tl, Token t) {
    if (tl->count == tl->cap) {
        int new_cap = tl->cap == 0 ? 32 : tl->cap * 2;
        Token *next = (Token *)realloc(tl->items, sizeof(Token) * new_cap);
        if (!next) return 0;
        tl->items = next; tl->cap = new_cap;
    }
    tl->items[tl->count++] = t;
    return 1;
}

static int is_operator_word(const char *s, TokenType *out) {
    if (strcmp(s, "and") == 0 || strcmp(s, "\xD0\xB8") == 0) { *out = TK_AND; return 1; }       // "и"
    if (strcmp(s, "or") == 0 || strcmp(s, "\xD0\xB8\xD0\xBB\xD0\xB8") == 0) { *out = TK_OR; return 1; } // "или"
    if (strcmp(s, "not") == 0 || strcmp(s, "\xD0\xBD\xD0\xB5") == 0) { *out = TK_NOT; return 1; } // "не"
    return 0;
}

static int token_is_term_or_rparen(const Token *t) {
    return t->type == TK_TERM || t->type == TK_RPAREN;
}

static int token_is_term_lparen_or_not(const Token *t) {
    return t->type == TK_TERM || t->type == TK_LPAREN || t->type == TK_NOT;
}

static int tokenize_query(const char *q, TokenList *out) {
    tl_init(out);
    size_t len = strlen(q);
    size_t i = 0;
    StrBuf sb;
    sb_init(&sb);
    Token prev;
    int has_prev = 0;

    while (i < len) {
        unsigned char c = (unsigned char)q[i];
        if (isspace(c)) { i++; continue; }
        if (c == '(') {
            Token t = {TK_LPAREN, NULL};
            if (has_prev && token_is_term_or_rparen(&prev)) {
                Token and_t = {TK_AND, NULL};
                tl_push(out, and_t);
            }
            tl_push(out, t);
            prev = t; has_prev = 1;
            i++;
            continue;
        }
        if (c == ')') {
            Token t = {TK_RPAREN, NULL};
            tl_push(out, t);
            prev = t; has_prev = 1;
            i++;
            continue;
        }

        sb_clear(&sb);
        while (i < len) {
            unsigned char ch = (unsigned char)q[i];
            if (is_ascii_token_char(ch)) { sb_push(&sb, (const char *)&ch, 1); i++; continue; }
            int cy = utf8_cyrillic_len((const unsigned char *)q + i, len - i);
            if (cy == 2) { sb_push(&sb, q + i, 2); i += 2; continue; }
            if (is_internal_joiner(ch)) {
                if (sb.len > 0) { sb_push(&sb, (const char *)&ch, 1); i++; continue; }
            }
            break;
        }

        if (sb.len > 0) {
            lowercase_utf8_inplace(sb.data);
            TokenType op;
            if (is_operator_word(sb.data, &op)) {
                Token t = {op, NULL};
                if (has_prev && token_is_term_or_rparen(&prev) && t.type == TK_NOT) {
                    Token and_t = {TK_AND, NULL};
                    tl_push(out, and_t);
                }
                tl_push(out, t);
                prev = t; has_prev = 1;
            } else {
                stem_ru(sb.data);
                Token t;
                t.type = TK_TERM;
                t.text = strdup(sb.data);
                if (has_prev && token_is_term_or_rparen(&prev)) {
                    Token and_t = {TK_AND, NULL};
                    tl_push(out, and_t);
                }
                tl_push(out, t);
                prev = t; has_prev = 1;
            }
            continue;
        }
        i++;
    }

    sb_free(&sb);
    return 1;
}

typedef struct {
    Token *items; int count; int cap;
} TokenStack;

static void ts_init(TokenStack *s) { s->items = NULL; s->count = 0; s->cap = 0; }
static void ts_free(TokenStack *s) { free(s->items); s->items = NULL; s->count = 0; s->cap = 0; }
static int ts_push(TokenStack *s, Token t) {
    if (s->count == s->cap) {
        int new_cap = s->cap == 0 ? 32 : s->cap * 2;
        Token *next = (Token *)realloc(s->items, sizeof(Token) * new_cap);
        if (!next) return 0;
        s->items = next; s->cap = new_cap;
    }
    s->items[s->count++] = t;
    return 1;
}
static Token ts_pop(TokenStack *s) { return s->items[--s->count]; }
static Token *ts_top(TokenStack *s) { return s->count ? &s->items[s->count - 1] : NULL; }

static int precedence(TokenType t) {
    if (t == TK_NOT) return 3;
    if (t == TK_AND) return 2;
    if (t == TK_OR) return 1;
    return 0;
}

static int to_postfix(const TokenList *in, TokenList *out) {
    tl_init(out);
    TokenStack ops; ts_init(&ops);

    for (int i = 0; i < in->count; ++i) {
        Token t = in->items[i];
        if (t.type == TK_TERM) {
            Token nt = {TK_TERM, strdup(t.text)};
            tl_push(out, nt);
        } else if (t.type == TK_AND || t.type == TK_OR || t.type == TK_NOT) {
            while (ops.count) {
                Token *top = ts_top(&ops);
                if (top->type == TK_LPAREN) break;
                if (precedence(top->type) >= precedence(t.type)) {
                    Token popt = ts_pop(&ops);
                    Token nt = {popt.type, NULL};
                    tl_push(out, nt);
                } else break;
            }
            ts_push(&ops, t);
        } else if (t.type == TK_LPAREN) {
            ts_push(&ops, t);
        } else if (t.type == TK_RPAREN) {
            while (ops.count && ts_top(&ops)->type != TK_LPAREN) {
                Token popt = ts_pop(&ops);
                Token nt = {popt.type, NULL};
                tl_push(out, nt);
            }
            if (ops.count && ts_top(&ops)->type == TK_LPAREN) ts_pop(&ops);
        }
    }

    while (ops.count) {
        Token popt = ts_pop(&ops);
        if (popt.type == TK_LPAREN || popt.type == TK_RPAREN) continue;
        Token nt = {popt.type, NULL};
        tl_push(out, nt);
    }

    ts_free(&ops);
    return 1;
}

// --- Boolean ops ---
static IntList list_intersect(const IntList *a, const IntList *b) {
    IntList out; il_init(&out);
    int i = 0, j = 0;
    while (i < a->count && j < b->count) {
        int va = a->items[i];
        int vb = b->items[j];
        if (va == vb) { il_push(&out, va); i++; j++; }
        else if (va < vb) i++; else j++;
    }
    return out;
}

static IntList list_union(const IntList *a, const IntList *b) {
    IntList out; il_init(&out);
    int i = 0, j = 0;
    while (i < a->count || j < b->count) {
        int va = (i < a->count) ? a->items[i] : INT32_MAX;
        int vb = (j < b->count) ? b->items[j] : INT32_MAX;
        if (va == vb) { if (va != INT32_MAX) il_push(&out, va); i++; j++; }
        else if (va < vb) { il_push(&out, va); i++; }
        else { il_push(&out, vb); j++; }
    }
    return out;
}

static IntList list_diff_all(const IntList *all, const IntList *b) {
    IntList out; il_init(&out);
    int i = 0, j = 0;
    while (i < all->count) {
        int va = all->items[i];
        int vb = (j < b->count) ? b->items[j] : INT32_MAX;
        if (va == vb) { i++; j++; }
        else if (va < vb) { il_push(&out, va); i++; }
        else { j++; }
    }
    return out;
}

// --- Evaluate ---
typedef struct { IntList *items; int count; int cap; } ListStack;

static void ls_init(ListStack *s) { s->items = NULL; s->count = 0; s->cap = 0; }
static void ls_free(ListStack *s) { for (int i = 0; i < s->count; ++i) il_free(&s->items[i]); free(s->items); s->items = NULL; s->count = 0; s->cap = 0; }
static int ls_push(ListStack *s, IntList v) {
    if (s->count == s->cap) {
        int new_cap = s->cap == 0 ? 32 : s->cap * 2;
        IntList *next = (IntList *)realloc(s->items, sizeof(IntList) * new_cap);
        if (!next) return 0;
        s->items = next; s->cap = new_cap;
    }
    s->items[s->count++] = v;
    return 1;
}
static IntList ls_pop(ListStack *s) { return s->items[--s->count]; }

static IntList evaluate_query(const TokenList *postfix, HashTable *ht, const IntList *all_docs) {
    ListStack st; ls_init(&st);

    for (int i = 0; i < postfix->count; ++i) {
        Token t = postfix->items[i];
        if (t.type == TK_TERM) {
            TermNode *node = ht_get(ht, t.text);
            IntList list; il_init(&list);
            if (node) {
                for (int k = 0; k < node->postings.count; ++k) il_push(&list, node->postings.items[k]);
            }
            ls_push(&st, list);
        } else if (t.type == TK_NOT) {
            IntList a = ls_pop(&st);
            IntList r = list_diff_all(all_docs, &a);
            il_free(&a);
            ls_push(&st, r);
        } else if (t.type == TK_AND || t.type == TK_OR) {
            IntList b = ls_pop(&st);
            IntList a = ls_pop(&st);
            IntList r = (t.type == TK_AND) ? list_intersect(&a, &b) : list_union(&a, &b);
            il_free(&a); il_free(&b);
            ls_push(&st, r);
        }
    }

    IntList result; il_init(&result);
    if (st.count > 0) result = ls_pop(&st);
    ls_free(&st);
    return result;
}

static void build_all_docs(const DocList *docs, IntList *out) {
    il_init(out);
    for (int i = 0; i < docs->count; ++i) il_push(out, docs->items[i].id);
}

