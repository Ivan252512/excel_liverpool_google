from flask import Flask, render_template, request, jsonify, Response
from flask_pymongo import pymongo
from func import Excel
from bson.json_util import dumps

SETTINGS = {
    
}

app = Flask(__name__, static_folder="static", template_folder="templates")

CONNECTION_STRING = "mongodb+srv://ivan:sarampion25@cluster0-s8nin.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(CONNECTION_STRING)
db = client.get_database('excel')
documents_collection = pymongo.collection.Collection(db, 'documents')
pages_collection = pymongo.collection.Collection(db, 'pages')
data_collection = pymongo.collection.Collection(db, 'data')
rows_collection = pymongo.collection.Collection(db, 'rows')

def to_db(src="ArchivosP.xlsx"):
    db.drop_collection("documents")
    db.drop_collection("pages")
    db.drop_collection("data")
    db.drop_collection("rows")
    
    excel = Excel(src)

    all_columns = excel.load_as_array()

    documents_to_save = {
        "name" : str(src)
    }

    documents_collection.insert(documents_to_save, check_keys=False)
    del documents_to_save

    sig=False
    title = []
    for i in all_columns:
        pages_to_save = {
            "document" : str(src),
            "name": ""
        }
        pages_to_save["name"] = str(i[0])
        pages_collection.insert(pages_to_save, check_keys=False)
        del pages_to_save
        for j in i[1]:
            row_to_save = {
                "page" : ""
            }
            data_to_save = {
                "page" : ""
            }
            if sig:
                title = j
                sig=False
            if j[0] == "vacio":
                sig=True
            else:
                is_row_title = False
                row_to_save["page"] = str(i[0])
                data_to_save["page"] = str(i[0])
                for x in range(len(title)):
                    if str(title[x]) == str(j[x]):
                        row_to_save[str(title[x])] = str(j[x])
                        is_row_title = True
                    else:
                        data_to_save[str(title[x])] = str(j[x])
                        is_row_title = False
                if is_row_title:
                    if "None" in row_to_save and len(row_to_save)<3:
                        del row_to_save
                    else:
                        rows_collection.insert(row_to_save, check_keys=False)
                        del row_to_save
                else:
                    data_collection.insert(data_to_save, check_keys=False)
                    del data_to_save

@app.route("/", methods=['GET'])
def home():
    return render_template("index.html")

@app.route("/load_database", methods=['POST'])
def load_database():
    to_db()
    return jsonify({"response": "ok"})

@app.route("/tables", methods=['GET'])
def tables():
    pages = pages_collection.find({})
    return render_template("tables.html", 
                            pages = pages)

@app.route("/page", methods=['POST'])
def rows():
    content = request.get_json(silent=True)
    page = content['page']

    data = data_collection.find({"page":page}, {'_id': False, "page":False})

    return Response(dumps(data), mimetype='application/json')




if __name__ == "__main__":
    app.run()