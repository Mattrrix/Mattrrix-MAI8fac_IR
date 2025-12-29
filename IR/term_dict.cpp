#include <vector>
#include <string>
#include <cstdint>

using namespace std;

struct Entry {
    string key;
    vector<int> postings;
    int next;
};

struct TermDict {
    vector<int> head;
    vector<Entry> pool;

    explicit TermDict(int buckets = 1 << 20);
    int find_or_add(const string& term);
    int find(const string& term) const;

    static uint64_t fnv1a(const string& s);
};

uint64_t TermDict::fnv1a(const string& s) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < s.size(); i++) {
        h ^= (unsigned char)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

TermDict::TermDict(int buckets) {
    head.assign(buckets, -1);
    pool.reserve(200000);
}

int TermDict::find_or_add(const string& term) {
    uint64_t h = fnv1a(term);
    int b = (int)(h % head.size());
    int idx = head[b];
    while (idx != -1) {
        if (pool[idx].key == term) return idx;
        idx = pool[idx].next;
    }
    Entry e;
    e.key = term;
    e.next = head[b];
    pool.push_back(e);
    head[b] = (int)pool.size() - 1;
    return (int)pool.size() - 1;
}

int TermDict::find(const string& term) const {
    uint64_t h = fnv1a(term);
    int b = (int)(h % head.size());
    int idx = head[b];
    while (idx != -1) {
        if (pool[idx].key == term) return idx;
        idx = pool[idx].next;
    }
    return -1;
}

static void add_posting(vector<int>& p, int doc_id) {
    if (p.empty() || p.back() != doc_id) p.push_back(doc_id);
}

void index_add(TermDict& dict, const string& term, int doc_id) {
    int idx = dict.find_or_add(term);
    add_posting(dict.pool[idx].postings, doc_id);
}
