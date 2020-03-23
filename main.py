from flask import Flask, render_template, request, jsonify, Response, redirect, make_response
from flask_pymongo import pymongo
from bson.json_util import dumps
from werkzeug.utils import secure_filename
import os
import pandas as pd

from google.cloud import storage
from openpyxl import load_workbook


app = Flask(__name__, static_folder="static", template_folder="templates")

# Configure this environment variable via app.yaml
CLOUD_STORAGE_BUCKET = "liverpool-excel.appspot.com"
#os.environ['CLOUD_STORAGE_BUCKET']

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

CONNECTION_STRING = "mongodb+srv://ivan:sarampion25@cluster0-s8nin.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(CONNECTION_STRING, maxPoolSize=10000)
db = client.get_database('excel')
documents_collection = pymongo.collection.Collection(db, 'documents')

class Excel():
    def __init__(self, name, url=None):
        self.url = url
        self.name = name
        try:
            self.df = pd.read_excel(url, None)
        except ValueError:
            self.df = pd.read_excel("CUENTAS_FUN_SOCIEDADES.xlsx", None)
        finally:
            self.sheets = self.df.keys()


    def load_to_db(self):
        sheets = {}
        k = 0
        for i in self.sheets:
            s = "sheet_"+str(k)
            sheets[s] = i
            k+=1
        documents_collection.insert_one({"name": self.name , 
                                         "url": self.url,
                                         "sheets": sheets})
    def get_sheets(self):
        return self.sheets

    def get_columns(self, sheet):
        df = self.df[sheet]
        return df.keys()

    def get_unique_column_values(self, sheet, column):
        df = self.df[sheet]
        return df[column].unique()

    def filter(self, sheet, filters):
        print(self.get_sheets())
        df = self.df[sheet]
        if type(sheet)==str and type(filters)==dict: 
            if len(filters)<1:
                return df, 500
            for i in filters.keys():
                if filters[i].isnumeric():
                    df =  df.loc[df[i] == int(filters[i])]
                else: 
                    df =  df.loc[df[i] == filters[i]]
            return df, 200
        return df, 500


def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/", methods=['GET'])
def home():
    return render_template("index.html")


@app.route("/tables/<document>/<sheet>", methods=['GET', 'POST'])
def tables(document, sheet):

    documents = documents_collection.find({})
    documents2 = documents_collection.find({})

    document_db = documents_collection.find_one({"name":document})

    try:
        sheets = list(document_db["sheets"].values())
        excel = Excel(document_db["name"], document_db["url"])
        if sheet == "CUENTAS" or sheet == "Seleccionar":
            sheet = sheets[0]
    except:
        excel = Excel(document)
        sheets = excel.get_sheets()

    
    doc, res = excel.filter(sheet,  1)

    del excel

    cols_values = []

    for i in doc.keys():
        cols_values.append([i, doc[i].unique()])

    table = doc.to_html()
    table = table.replace('<table border="1" class="dataframe">', '<table class="table table-bordered" id="dataTable" width="100%" cellspacing="0">')
    
    return render_template('tables.html',  
                            table=table,
                            documents=documents,
                            documents2=documents2,
                            sheets=sheets,
                            document_selected=document,
                            sheet_selected=sheet,
                            cols_values=cols_values)

@app.route("/get_pages/<document>", methods=['GET', 'POST'])
def get_pages(document):
    document = documents_collection.find_one({"name":document})
    return Response(dumps(document["sheets"]), mimetype='application/json')

@app.route("/tables_filter/<document>/<sheet>", methods=['GET', 'POST'])
def tables_filter(document, sheet):
    content = request.get_json()
    document_db = documents_collection.find_one({"name":document})

    excel = Excel(document_db["name"], document_db["url"])
    doc, res = excel.filter(str(sheet),  content)

    cols_values = []

    for i in doc.keys():
        cols_values.append([i, doc[i].unique()])

    table = doc.to_html()
    table = table.replace('<table border="1" class="dataframe">', '<table class="table table-bordered" id="dataTable" width="100%" cellspacing="0">')
    
    return table  


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

            excel = Excel(filename, blob.public_url)
            excel.load_to_db()
        return redirect("/tables/CUENTAS_FUN_SOCIEDADES.xlsx/CUENTAS")

@app.route('/drop_database', methods=['GET'])
def drop_database():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(CLOUD_STORAGE_BUCKET)
    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()

    documents_collection.drop()

    return redirect("/tables/CUENTAS_FUN_SOCIEDADES.xlsx/CUENTAS")

if __name__ == "__main__":
    app.run()