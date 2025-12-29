#include <vector>
#include <string>
#include <cstdint>

using namespace std;

static bool utf8_next(const string& s, size_t& i, uint32_t& cp) {
    if (i >= s.size()) return false;
    unsigned char c = (unsigned char)s[i];

    if (c < 0x80) { cp = c; i += 1; return true; }

    if ((c >> 5) == 0x6) {
        if (i + 1 >= s.size()) return false;
        cp = ((uint32_t)(c & 0x1F) << 6) | ((uint32_t)((unsigned char)s[i+1]) & 0x3F);
        i += 2; return true;
    }
    if ((c >> 4) == 0xE) {
        if (i + 2 >= s.size()) return false;
        cp = ((uint32_t)(c & 0x0F) << 12)
           | ((uint32_t)((unsigned char)s[i+1] & 0x3F) << 6)
           | ((uint32_t)((unsigned char)s[i+2] & 0x3F));
        i += 3; return true;
    }
    if ((c >> 3) == 0x1E) {
        if (i + 3 >= s.size()) return false;
        cp = ((uint32_t)(c & 0x07) << 18)
           | ((uint32_t)((unsigned char)s[i+1] & 0x3F) << 12)
           | ((uint32_t)((unsigned char)s[i+2] & 0x3F) << 6)
           | ((uint32_t)((unsigned char)s[i+3] & 0x3F));
        i += 4; return true;
    }

    i += 1;
    return false;
}

static void utf8_append(string& out, uint32_t cp) {
    if (cp < 0x80) out.push_back((char)cp);
    else if (cp < 0x800) {
        out.push_back((char)(0xC0 | (cp >> 6)));
        out.push_back((char)(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out.push_back((char)(0xE0 | (cp >> 12)));
        out.push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back((char)(0x80 | (cp & 0x3F)));
    } else {
        out.push_back((char)(0xF0 | (cp >> 18)));
        out.push_back((char)(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back((char)(0x80 | (cp & 0x3F)));
    }
}

static bool is_digit_cp(uint32_t cp) { return cp >= '0' && cp <= '9'; }
static bool is_latin_letter(uint32_t cp) { return (cp >= 'A' && cp <= 'Z') || (cp >= 'a' && cp <= 'z'); }
static bool is_cyrillic_letter(uint32_t cp) {
    if (cp == 0x0401 || cp == 0x0451) return true;
    return (cp >= 0x0410 && cp <= 0x044F);
}
static uint32_t to_lower_cp(uint32_t cp) {
    if (cp >= 'A' && cp <= 'Z') return cp + 32;
    if (cp >= 0x0410 && cp <= 0x042F) return cp + 0x20;
    if (cp == 0x0401) return 0x0451;
    return cp;
}
static bool is_word_cp(uint32_t cp) {
    return is_digit_cp(cp) || is_latin_letter(cp) || is_cyrillic_letter(cp);
}

vector<string> tokenize_utf8(const string& text) {
    vector<string> tokens;
    string cur;

    size_t i = 0;
    uint32_t cp = 0;
    while (utf8_next(text, i, cp)) {
        if (is_word_cp(cp)) {
            cp = to_lower_cp(cp);
            utf8_append(cur, cp);
        } else {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
        }
    }
    if (!cur.empty()) tokens.push_back(cur);
    return tokens;
}
