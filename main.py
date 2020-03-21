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

ALLOWED_EXTENSIONS = {'xlsx', 'csv', 'ods'}

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
        documents_collection.insert_one({"name": src})
        for i in self.get_pages():
            pages_collection.insert_one({"document": src, "page":str(i)})
        self.save_columns()



    def get_pages(self):
        return self.excel.sheetnames

    def save_columns(self):
        all_columns = []
        for j in self.get_pages():
            sheet = self.excel[j]
            row_to_save = {
                "page" : str(j),
            }
            for cell in sheet[1]:
                if True:
                    row_to_save[str(cell.value)]=str(cell.value)
            rows_collection.insert_one(row_to_save)

    def _process_page(self, i):
        if i==1:
            self.row_v_l =[]
            for cell in self.sheet[1]:
                self.row_v_l.append(str(cell.value))
        else:
            data_to_save = {
                "page": str(self.page),
            }
            cont_r = 0
            for cell in self.sheet[i]:
                if cont_r<len(self.row_v_l):
                    data_to_save[str(cell.value)]=str(cell.value)
                cont_r+=1
            data_collection.insert_one(data_to_save)
            
    def load_to_db(self):
        for j in self.get_pages():
            self.page = str(j)
            self.sheet = self.excel[self.page]
            for i in range(1,self.sheet.max_row):
                if i==1:
                    self.row_v_l =[]
                    for cell in self.sheet[1]:
                        self.row_v_l.append(str(cell.value))
                else:
                    data_to_save = {
                        "page": str(self.page),
                    }
                    cont_r = 0
                    for cell in self.sheet[i]:
                        if cont_r<len(self.row_v_l):
                            data_to_save[str(cell.value)]=str(cell.value)
                        cont_r+=1
                    try:
                        print(data_to_save)
                        data_collection.insert_one(data_to_save)
                    except Exception as e:
                        print(e)



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
            excel = Excel(file)
            excel.load_to_db()
            return redirect('tables')
        else:
            return redirect('tables')

@app.route("/tables", methods=['GET'])
def tables():
    return render_template("tables.html")

@app.route('/documents', methods=['GET'])
def documents():
    documents = documents_collection.find({}, {'_id': False})
    return Response(dumps(documents), mimetype='application/json')

@app.route('/pages', methods=['POST'])
def pages():
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