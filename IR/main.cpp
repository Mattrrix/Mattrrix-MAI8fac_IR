#include <iostream>
#include <vector>
#include <string>
#include <fstream>

using namespace std;

#include "decl.cpp"

vector<string> tokenize_utf8(const string& text);
string stem(const string& token);

void read_corpus(const string& path, vector<string>& doc_ids, vector<string>& texts);
void save_index(const string& path, const vector<string>& doc_ids, const TermDict& dict);
void load_index(const string& path, vector<string>& doc_ids, TermDict& dict);

vector<int> eval_boolean_query(const string& query, const TermDict& dict, const vector<int>& Universe);

static void build_index_from_texts(const vector<string>& texts, TermDict& dict) {
    for (int doc_id = 0; doc_id < (int)texts.size(); doc_id++) {
        vector<string> toks = tokenize_utf8(texts[doc_id]);
        for (size_t i = 0; i < toks.size(); i++) {
            string t = stem(toks[i]);
            if (t.size() < 2) continue;
            index_add(dict, t, doc_id);
        }
    }
}

static void ui_loop(const string& index_path, const string& corpus_path) {
    vector<string> doc_ids;
    TermDict dict(1 << 20);
    load_index(index_path, doc_ids, dict);

    vector<int> Universe;
    Universe.reserve(doc_ids.size());
    for (int i = 0; i < (int)doc_ids.size(); i++) Universe.push_back(i);

    cout << "Local Boolean Search UI\n";
    cout << "Operators: AND OR NOT, parentheses: ( )\n";
    cout << "Commands:\n";
    cout << "  :open N    open document by result number (from last search)\n";
    cout << "  :exit\n";

    vector<int> last;

    vector<string> ids2;
    vector<string> texts;
    read_corpus(corpus_path, ids2, texts);

    string q;
    while (true) {
        cout << "\nsearch> ";
        if (!getline(cin, q)) break;
        if (q == ":exit" || q == "exit" || q == "quit") break;

        if (q.size() > 6 && q.substr(0, 6) == ":open ") {
            int n = atoi(q.substr(6).c_str());
            if (n <= 0 || n > (int)last.size()) {
                cout << "bad N\n";
                continue;
            }
            int doc_id = last[n - 1];
            cout << "\nDOC_ID: " << doc_ids[doc_id] << "\n";
            cout << "------------------------------\n";
            if (doc_id >= 0 && doc_id < (int)texts.size()) {
                string t = texts[doc_id];
                if (t.size() > 3000) t = t.substr(0, 3000) + "\n...[cut]\n";
                cout << t << "\n";
            } else {
                cout << "(text not found in corpus dump)\n";
            }
            continue;
        }

        last = eval_boolean_query(q, dict, Universe);
        cout << "hits: " << last.size() << "\n";

        int show = (int)last.size() < 20 ? (int)last.size() : 20;
        for (int i = 0; i < show; i++) {
            cout << (i + 1) << ") " << doc_ids[last[i]] << "\n";
        }
    }
}

int main(int argc, char** argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    if (argc < 3) {
        cerr << "usage:\n";
        cerr << "  " << argv[0] << " build <corpus_dump.txt> <boolean_index.bin>\n";
        cerr << "  " << argv[0] << " ui <boolean_index.bin> <corpus_dump.txt>\n";
        return 1;
    }

    string mode = argv[1];

    try {
        if (mode == "build") {
            if (argc < 4) return 1;
            string corpus_path = argv[2];
            string index_path = argv[3];

            vector<string> doc_ids;
            vector<string> texts;
            read_corpus(corpus_path, doc_ids, texts);

            TermDict dict(1 << 20);
            build_index_from_texts(texts, dict);

            save_index(index_path, doc_ids, dict);
            cout << "OK\n";
        } else if (mode == "ui") {
            if (argc < 4) return 1;
            ui_loop(argv[2], argv[3]);
        } else {
            return 1;
        }
    } catch (const exception& e) {
        cerr << "error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
