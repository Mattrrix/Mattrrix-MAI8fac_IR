#include <vector>
#include <string>

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

    static unsigned long long fnv1a(const string& s);
};

void index_add(TermDict& dict, const string& term, int doc_id);
