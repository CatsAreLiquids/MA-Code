import json

def add_doc(db, file, type):
    data = json.load(open(file))
    docs = []
    for key in data.keys():
        tags = data[key]["tags"]
        tags2 = ','.join(tags)
        meta_dict = {"tags": tags}
        meta_dict["tags2"] = tags2
        meta_dict["type"] = type
        meta_dict["file"] = key
        meta_dict["min_year"] = data[key]["min_year"]
        meta_dict["max_year"] = data[key]["max_year"]
        docs.append(Document(page_content=data[key]["description"], metadata=meta_dict))
    db.add_documents(docs)
