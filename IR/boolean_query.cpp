#include "decl.cpp"

static vector<int> op_and(const vector<int>& A, const vector<int>& B) {
    vector<int> out;
    size_t i = 0, j = 0;
    while (i < A.size() && j < B.size()) {
        if (A[i] == B[j]) { out.push_back(A[i]); i++; j++; }
        else if (A[i] < B[j]) i++;
        else j++;
    }
    return out;
}

static vector<int> op_or(const vector<int>& A, const vector<int>& B) {
    vector<int> out;
    size_t i = 0, j = 0;
    while (i < A.size() || j < B.size()) {
        if (j >= B.size() || (i < A.size() && A[i] < B[j])) out.push_back(A[i++]);
        else if (i >= A.size() || (j < B.size() && B[j] < A[i])) out.push_back(B[j++]);
        else { out.push_back(A[i]); i++; j++; }
    }
    return out;
}

static vector<int> op_not(const vector<int>& U, const vector<int>& A) {
    vector<int> out;
    size_t i = 0, j = 0;
    while (i < U.size()) {
        if (j >= A.size() || U[i] < A[j]) { out.push_back(U[i]); i++; }
        else if (U[i] == A[j]) { i++; j++; }
        else j++;
    }
    return out;
}

enum TokType { TT_TERM, TT_AND, TT_OR, TT_NOT, TT_LP, TT_RP };
struct Tok { TokType t; string v; };

static bool is_space(char c) { return c==' '||c=='\t'||c=='\n'||c=='\r'; }

vector<string> tokenize_utf8(const string& text);
string stem(const string& token);

static vector<Tok> lex_query(const string& q) {
    vector<Tok> toks;
    size_t i = 0;
    while (i < q.size()) {
        while (i < q.size() && is_space(q[i])) i++;
        if (i >= q.size()) break;

        if (q[i] == '(') { toks.push_back({TT_LP, ""}); i++; continue; }
        if (q[i] == ')') { toks.push_back({TT_RP, ""}); i++; continue; }

        size_t j = i;
        while (j < q.size() && !is_space(q[j]) && q[j] != '(' && q[j] != ')') j++;
        string w = q.substr(i, j - i);

        string u;
        for (char ch : w) {
            if (ch >= 'a' && ch <= 'z') u.push_back(ch - 32);
            else u.push_back(ch);
        }

        if (u == "AND") toks.push_back({TT_AND, ""});
        else if (u == "OR") toks.push_back({TT_OR, ""});
        else if (u == "NOT") toks.push_back({TT_NOT, ""});
        else {
            vector<string> t = tokenize_utf8(w);
            if (!t.empty()) toks.push_back({TT_TERM, stem(t[0])});
        }

        i = j;
    }
    return toks;
}

static int prec(TokType t) {
    if (t == TT_NOT) return 3;
    if (t == TT_AND) return 2;
    if (t == TT_OR) return 1;
    return 0;
}

static bool right_assoc(TokType t) { return t == TT_NOT; }

static vector<Tok> to_rpn(const vector<Tok>& in) {
    vector<Tok> out;
    vector<Tok> st;

    for (auto tok : in) {
        if (tok.t == TT_TERM) out.push_back(tok);
        else if (tok.t == TT_AND || tok.t == TT_OR || tok.t == TT_NOT) {
            while (!st.empty()) {
                Tok top = st.back();
                if (top.t == TT_LP) break;
                int p1 = prec(tok.t), p2 = prec(top.t);
                if (p2 > p1 || (p2 == p1 && !right_assoc(tok.t))) {
                    out.push_back(top);
                    st.pop_back();
                } else break;
            }
            st.push_back(tok);
        } else if (tok.t == TT_LP) st.push_back(tok);
        else if (tok.t == TT_RP) {
            while (!st.empty() && st.back().t != TT_LP) {
                out.push_back(st.back());
                st.pop_back();
            }
            if (!st.empty() && st.back().t == TT_LP) st.pop_back();
        }
    }
    while (!st.empty()) {
        if (st.back().t != TT_LP) out.push_back(st.back());
        st.pop_back();
    }
    return out;
}

vector<int> eval_boolean_query(const string& query, const TermDict& dict, const vector<int>& Universe) {
    vector<Tok> rpn = to_rpn(lex_query(query));
    vector< vector<int> > st;

    for (const auto& tok : rpn) {
        if (tok.t == TT_TERM) {
            int idx = dict.find(tok.v);
            if (idx == -1) st.push_back(vector<int>());
            else st.push_back(dict.pool[idx].postings);
        } else if (tok.t == TT_NOT) {
            if (st.empty()) return vector<int>();
            vector<int> A = st.back(); st.pop_back();
            st.push_back(op_not(Universe, A));
        } else if (tok.t == TT_AND) {
            if (st.size() < 2) return vector<int>();
            vector<int> B = st.back(); st.pop_back();
            vector<int> A = st.back(); st.pop_back();
            st.push_back(op_and(A, B));
        } else if (tok.t == TT_OR) {
            if (st.size() < 2) return vector<int>();
            vector<int> B = st.back(); st.pop_back();
            vector<int> A = st.back(); st.pop_back();
            st.push_back(op_or(A, B));
        }
    }

    if (st.empty()) return vector<int>();
    return st.back();
}
