from flask import Flask, render_template, request, jsonify, Response, redirect
from flask_pymongo import pymongo
from bson.json_util import dumps
from werkzeug.utils import secure_filename
import os

from google.cloud import storage
from openpyxl import load_workbook
from multiprocessing import Pool


app = Flask(__name__, static_folder="static", template_folder="templates")

# Configure this environment variable via app.yaml
CLOUD_STORAGE_BUCKET = "liverpoolexcelprueba.appspot.com"
#os.environ['CLOUD_STORAGE_BUCKET']

CONNECTION_STRING = "mongodb+srv://ivan:sarampion25@cluster0-s8nin.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(CONNECTION_STRING, maxPoolSize=10000)
db = client.get_database('excel')
documents_collection = pymongo.collection.Collection(db, 'documents')
pages_collection = pymongo.collection.Collection(db, 'pages')
data_collection = pymongo.collection.Collection(db, 'data')
rows_collection = pymongo.collection.Collection(db, 'rows')

class Excel():
    
    def __init__(self, src):
        self.excel = load_workbook(src, data_only=True)
        self.name = str(src)
        self.sheet = None
        self.page = None
        self.row_v_l =[]

    def get_pages(self):
        return self.excel.sheetnames

    def load_as_array(self):
        all_columns = []
        for j in self.get_pages():
            sheet_column = [j, []]
            sheet = self.excel[j]
            sheet_column[1].append(["vacio"])
            for i in range(1,sheet.max_row):
                sheet_column[1].append([cell.value for cell in sheet[i]])
            all_columns.append(sheet_column)
        return all_columns   


    def _process_page(self, i):
        if i==1:
            self.row_v_l =[]
            row_to_save = {
                "page" : str(self.page),
            }
            for cell in self.sheet[1]:
                row_to_save[str(cell.value)]=str(cell.value)
                self.row_v_l.append(str(cell.value))
            rows_collection.insert_one(row_to_save)
        else:
            data_to_save = {
                "page": str(self.page),
            }
            cont_r = 0
            for cell in self.sheet[i]:
                if cont_r<len(self.row_v_l):
                    data_to_save[self.row_v_l[cont_r]]=str(cell.value)
                cont_r+=1
            data_collection.insert_one(data_to_save)

    def load_to_db(self):
        documents_to_save = {
            "name" : self.name
        }

        documents_collection.insert_one(documents_to_save)

        for j in self.get_pages():
            self.page = j
            pages_to_save = {
                "document" : self.name,
                "page": str(j)
            }
            pages_collection.insert_one(pages_to_save)

            self.sheet = self.excel[self.page]

            try:
                pool = Pool()
                pool.map(self._process_page, range(1,self.sheet.max_row))
            except Exception as e:
                print(e)
            finally: 
                pool.close()
                pool.join()



def to_db(src):
    #db.drop_collection("documents")
    #db.drop_collection("pages")
    #db.drop_collection("data")
    #db.drop_collection("rows")
    
    excel = Excel(src)

    all_columns = excel.load_as_array()

    documents_to_save = {
        "name" : str(src)
    }

    documents_collection.insert_one(documents_to_save)
    del documents_to_save

    sig=False
    title = []
    for i in all_columns:
        pages_to_save = {
            "document" : str(src),
            "name": ""
        }
        pages_to_save["name"] = str(i[0])
        pages_collection.insert_one(pages_to_save)
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
                        rows_collection.insert_one(row_to_save)
                        del row_to_save
                else:
                    data_collection.insert_one(data_to_save)
                    del data_to_save

@app.route("/", methods=['GET'])
def home():
    return render_template("index.html")

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/load_database_w', methods=['POST'])
def load_database_w(src="CUENTAS_FUN_SOCIEDADES.xlsx"):
    excel =  Excel(src)
    excel.load_to_db()
    return jsonify({"response": "ok"})

@app.route('/load_database', methods=['POST'])
def load_database():
    if request.method == 'POST':
        if 'file' not in request.files:
            return redirect('tables')
        file = request.files['file']
        if file.filename == '':
            return redirect('tables')
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)

            # Create a Cloud Storage client.
            gcs = storage.Client()

            # Get the bucket that the file will be uploaded to.
            bucket = gcs.get_bucket(CLOUD_STORAGE_BUCKET)

            # Create a new blob and upload the file's content.
            blob = bucket.blob(filename)

            blob.upload_from_string(
                file.read(),
                content_type=file.content_type
            )

            documents_collection.insert_one({"name": filename, 
                                         "url": blob.public_url})
            return redirect('tables')
        else:
            return redirect('tables')

@app.route("/tables", methods=['GET'])
def tables():
    documents = documents_collection.find({})
    return render_template("tables.html", 
                            documents = documents)

@app.route('/documents', methods=['POST'])
def documents():
    content = request.get_json()
    document = content["document"]
    page = pages_collection.find({"document":document}, {'_id': False})
    return Response(dumps(page), mimetype='application/json')

@app.route("/rows", methods=['POST'])
def rows():
    content = request.get_json()
    print(content)
    data = data_collection.find(content, {'_id': False, "page":False})
    return Response(dumps(data), mimetype='application/json')

if __name__ == "__main__":
    app.run()