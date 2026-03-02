#include "common.h"

static void print_usage(const char *prog) {
    printf("Usage: %s --index index.tsv --docs docs.tsv --query \"...\"\n", prog);
}

int main(int argc, char **argv) {
    const char *index_path = NULL;
    const char *docs_path = NULL;
    const char *query = NULL;

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--index") == 0 && i + 1 < argc) index_path = argv[++i];
        else if (strcmp(argv[i], "--docs") == 0 && i + 1 < argc) docs_path = argv[++i];
        else if (strcmp(argv[i], "--query") == 0 && i + 1 < argc) query = argv[++i];
        else { print_usage(argv[0]); return 1; }
    }

    if (!index_path || !docs_path || !query) { print_usage(argv[0]); return 1; }

    HashTable ht; ht_init(&ht, 200003);
    DocList docs; doclist_init(&docs);

    if (!load_docs(docs_path, &docs)) { fprintf(stderr, "Failed to load docs\n"); return 1; }
    if (!load_index(index_path, &ht)) { fprintf(stderr, "Failed to load index\n"); return 1; }

    IntList all_docs; build_all_docs(&docs, &all_docs);

    TokenList tokens; tokenize_query(query, &tokens);
    TokenList postfix; to_postfix(&tokens, &postfix);
    IntList result = evaluate_query(&postfix, &ht, &all_docs);

    printf("Results: %d\n", result.count);
    for (int i = 0; i < result.count; ++i) {
        int id = result.items[i];
        if (id >= 0 && id < docs.count) {
            printf("%d\t%s\n", id, docs.items[id].url ? docs.items[id].url : "");
        }
    }

    il_free(&result);
    il_free(&all_docs);
    tl_free(&tokens);
    tl_free(&postfix);
    ht_free(&ht);
    doclist_free(&docs);

    return 0;
}
