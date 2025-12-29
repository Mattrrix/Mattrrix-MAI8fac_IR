#include "decl.cpp"
#include <fstream>
#include <cstdint>

static bool read_line(ifstream& in, string& line) {
    line.clear();
    return (bool)getline(in, line);
}

void read_corpus(const string& path, vector<string>& doc_ids, vector<string>& texts) {
    ifstream in(path);
    if (!in) throw runtime_error("cannot open corpus file");

    string line, cur_id, cur_text;

    while (read_line(in, line)) {
        if (line == "==DOC_START==") {
            cur_id.clear();
            cur_text.clear();

            read_line(in, cur_id);     // id
            read_line(in, line);     

            while (read_line(in, line)) {
                if (line == "==DOC_END==") break;
                cur_text += line;
                cur_text.push_back('\n');
            }

            doc_ids.push_back(cur_id);
            texts.push_back(cur_text);
        }
    }
}

static void write_u32(ofstream& out, uint32_t v) { out.write((char*)&v, sizeof(v)); }
static void write_i32(ofstream& out, int32_t v) { out.write((char*)&v, sizeof(v)); }
static bool read_u32(ifstream& in, uint32_t& v) { return (bool)in.read((char*)&v, sizeof(v)); }
static bool read_i32(ifstream& in, int32_t& v) { return (bool)in.read((char*)&v, sizeof(v)); }

void save_index(const string& path, const vector<string>& doc_ids, const TermDict& dict) {
    ofstream out(path, ios::binary);
    if (!out) throw runtime_error("cannot open index for write");

    out.write("BIDX1", 5);

    write_u32(out, (uint32_t)doc_ids.size());
    for (const auto& id : doc_ids) {
        write_u32(out, (uint32_t)id.size());
        out.write(id.data(), id.size());
    }

    write_u32(out, (uint32_t)dict.pool.size());
    for (const auto& e : dict.pool) {
        write_u32(out, (uint32_t)e.key.size());
        out.write(e.key.data(), e.key.size());

        write_u32(out, (uint32_t)e.postings.size());
        for (int d : e.postings) write_i32(out, (int32_t)d);
    }
}

void load_index(const string& path, vector<string>& doc_ids, TermDict& dict) {
    ifstream in(path, ios::binary);
    if (!in) throw runtime_error("cannot open index for read");

    char magic[5];
    in.read(magic, 5);
    if (string(magic, magic + 5) != "BIDX1") throw runtime_error("bad index format");

    uint32_t n_docs = 0;
    read_u32(in, n_docs);

    doc_ids.clear();
    doc_ids.reserve(n_docs);

    for (uint32_t i = 0; i < n_docs; i++) {
        uint32_t len = 0;
        read_u32(in, len);
        string id(len, '\0');
        in.read(&id[0], len);
        doc_ids.push_back(id);
    }

    uint32_t n_terms = 0;
    read_u32(in, n_terms);

    dict = TermDict(1 << 20);
    dict.pool.reserve(n_terms);

    for (uint32_t i = 0; i < n_terms; i++) {
        uint32_t klen = 0;
        read_u32(in, klen);

        string key(klen, '\0');
        in.read(&key[0], klen);

        int idx = dict.find_or_add(key);

        uint32_t plen = 0;
        read_u32(in, plen);

        dict.pool[idx].postings.clear();
        dict.pool[idx].postings.reserve(plen);

        for (uint32_t j = 0; j < plen; j++) {
            int32_t d = 0;
            read_i32(in, d);
            dict.pool[idx].postings.push_back((int)d);
        }
    }
}
