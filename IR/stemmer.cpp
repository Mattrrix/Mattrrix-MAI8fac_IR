#include <vector>
#include <string>

using namespace std;

static bool ends_with(const string& s, const string& suf) {
    if (s.size() < suf.size()) return false;
    return s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
}

static void strip_suffix(string& w, const vector<string>& sufs, int min_len) {
    for (const auto& suf : sufs) {
        if ((int)w.size() > (int)suf.size() + min_len && ends_with(w, suf)) {
            w.erase(w.size() - suf.size());
            return;
        }
    }
}

string stem(const string& token) {
    string w = token;
    if (w.size() < 3) return w;

    static vector<string> en = {
        "ization","ational","fulness","ousness","iveness","tional",
        "ation","alism","aliti","ousli","iviti","ing","ed","ly","es","s"
    };
    static vector<string> ru = {
        "иями","ями","ами","ение","ениями","ениях","ением",
        "остью","ости","ость","ению","ения","ений",
        "кого","его","ому","ему","ыми","ими",
        "ая","яя","ое","ее","ые","ие","ой","ей","ым","им",
        "ах","ях","ам","ям","ом","ем","ов","ев","ою","ею",
        "а","я","о","е","ы","и","у","ю","ь"
    };

    bool has_ru = false;
    for (size_t i = 0; i < w.size(); i++) {
        if ((unsigned char)w[i] >= 0xD0) { has_ru = true; break; }
    }

    if (has_ru) strip_suffix(w, ru, 2);
    else strip_suffix(w, en, 2);

    return w.empty() ? token : w;
}
