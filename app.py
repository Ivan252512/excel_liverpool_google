from flask import Flask, render_template, request, jsonify, Response, redirect
from flask_pymongo import pymongo
from bson.json_util import dumps
from werkzeug.utils import secure_filename
import os
import pandas as pd

from google.cloud import storage
from openpyxl import load_workbook


app = Flask(__name__, static_folder="static", template_folder="templates")

# Configure this environment variable via app.yaml
CLOUD_STORAGE_BUCKET = "liverpoolexcelprueba.appspot.com"
#os.environ['CLOUD_STORAGE_BUCKET']

ALLOWED_EXTENSIONS = {'xlsx', 'csv', 'ods'}

CONNECTION_STRING = "mongodb+srv://ivan:sarampion25@cluster0-s8nin.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(CONNECTION_STRING, maxPoolSize=10000)
db = client.get_database('excel')
documents_collection = pymongo.collection.Collection(db, 'documents')

class Excel():
    def __init__(self, src):
        self.df = pd.read_excel(src, None)
        self.name = src
        self.sheets = self.df.keys()

    def get_sheets(self):
        return self.sheets

    def get_columns(self, sheet):
        df = self.df[sheet]
        return df.keys()

    def get_unique_column_values(self, sheet, column):
        df = self.df[sheet]
        return df[column].unique()

    def filter(self, sheet, column, values):
        df = self.df[sheet]
        if type(sheet)==type(column)==str and type(values)==list:
            return df.loc[df[column].isin(values)], 200
        return df, 500

    def complex_filter(self, sheet, filters):
        df = self.df[sheet]
        if type(sheet)==str and type(filters)==list:
            if len(filters)<1:
                return df, 500
            if not type(filters[0])==list:
                return df, 500
            if not len(filters[0])==2:
                return df, 500
            query_str = ""
            for i in filter:
                query_str+="{}=={} | ".format(i[0], i[1])
            query_str=query_str[:-3]
            return df.query(query_str), 200    
        return df, 500

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/", methods=['GET'])
def home():
    return render_template("index.html")

@app.route("/tables", methods=['GET'])
def tables():
    content = request.get_json()
    """
    document = content["document"] if "document" in content.keys() else None
    sheet = content["sheet"] if "sheet" in content.keys() else None
    """
    sheet = "CUENTAS"
    document = "CUENTAS_FUN_SOCIEDADES.xlsx"

    documents = [document]
    sheets = [sheet]

    excel = Excel(document)
    doc, res = excel.complex_filter(str(sheet), 1 )

    cols_values = []

    for i in doc.keys():
        cols_values.append([i, doc[i].unique()])

    table = doc.to_html()
    table = table.replace('<table border="1" class="dataframe">', '<table class="table table-bordered" id="dataTable" width="100%" cellspacing="0">')
    
    return render_template('tables.html',  
                            table=table,
                            documents=documents,
                            sheets=sheets,
                            cols_values=cols_values)

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



if __name__ == "__main__":
    app.run()