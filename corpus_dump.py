from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27018"
DB = "IR_MAI"
COLL = "labs_doc"
OUT = "corpus_dump.txt"

client = MongoClient(MONGO_URI)
col = client[DB][COLL]

with open(OUT, "w", encoding="utf-8") as f:
    for doc in col.find({}, {"_id": 1, "clean_text": 1}):
        text = doc.get("clean_text") or ""
        if not text.strip():
            continue
        f.write("==DOC_START==\n")
        f.write(str(doc["_id"]) + "\n")
        f.write("==CONTENT_START==\n")
        f.write(text.replace("\r", "") + "\n")
        f.write("==DOC_END==\n")

print("OK:", OUT)
