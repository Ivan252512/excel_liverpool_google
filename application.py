from flask import Flask, render_template, request, jsonify, Response, redirect, make_response, send_file
from flask_pymongo import pymongo
from bson.json_util import dumps
import os
import pandas as pd
import dask
import dask.dataframe as dd
from dask.delayed import delayed

from openpyxl import load_workbook
import logging
import boto3
from botocore.exceptions import ClientError

application = Flask(__name__, static_folder="static", template_folder="templates")
# Configure this environment variable via application.yaml
AWS_BUCKET_NAME = "liverpoolexcel"
UPLOAD_FOLDER = "/tmp"

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

CONNECTION_STRING = "mongodb+srv://ivan:sarampion25@cluster0-s8nin.mongodb.net/test?retryWrites=true&w=majority"
client = pymongo.MongoClient(CONNECTION_STRING, maxPoolSize=10000)
db = client.get_database('excel')
documents_collection = pymongo.collection.Collection(db, 'documents')

excels = {'Seleccionar' : pd.read_excel("Seleccionar.xlsx", None)}

for i in documents_collection.find({}):
    excels[i['name']] = pd.read_excel("https://liverpoolexcel.s3-us-west-1.amazonaws.com//tmp/"+i['name'], None, dtype=str)

class Excel:
    def __init__(self, name):
        self.name = name
        try:
            self.df = excels[name]
        except:
            self.df = excels['Seleccionar']
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
        parts = dask.delayed(self.df[sheet])
        dfd = dd.from_delayed(parts)
        df = dfd.compute()
        if type(sheet)==str and type(filters)==dict: 
            if len(filters)<1:
                return df, 500
            for i in filters.keys():
                if filters[i] in ["NaN", "nan", "NAN", "none", "None", None]:
                    df =  df.loc[df[i].isnull()]
                else:
                    df =  df.loc[df[i] == filters[i]]
            return df, 200
        return df, 500

def upload_file(file_name, bucket):
    object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
    return response


@application.route("/", methods=['GET', 'POST'])
def home():
    return tables("Seleccionar", "Seleccionar", 100)

@application.route("/<document>/<sheet>/<limit>", methods=['GET', 'POST'])
def tables(document, sheet, limit):

    document = str(document).replace("'", "")
    document = document.replace('"', "")

    sheet = str(sheet).replace("'", "")
    sheet = sheet.replace('"', "")

    documents = documents_collection.find({})
    documents2 = documents_collection.find({})
   
    

    excel = Excel(document)
    sheets = excel.get_sheets()

    doc, res = excel.filter(sheet,  1)

    del excel

    cols_values = []

    for i in doc.keys():
        cols_values.append([i, doc[i].unique()])

    limit = int(limit)
    if limit<=0:
        limit = 100

    table = doc[:limit].to_html()
    table = table.replace('<table border="1" class="dataframe">', '<table class="table table-bordered" id="dataTable" width="100%" cellspacing="0">')
    
    return render_template('tables.html',  
                            table=table,
                            documents=documents,
                            documents2=documents2,
                            sheets=sheets,
                            document_selected=document,
                            sheet_selected=sheet,
                            cols_values=cols_values,
                            limit=limit)

@application.route("/get_pages/<document>", methods=['GET', 'POST'])
def get_pages(document):
    document = documents_collection.find_one({"name":document})
    return Response(dumps(document["sheets"]), mimetype='application/json')

@application.route("/tables_filter/<document>/<sheet>", methods=['GET', 'POST'])
def tables_filter(document, sheet):
    document = str(document).replace("'", "")
    document = document.replace('"', "")

    sheet = str(sheet).replace("'", "")
    sheet = sheet.replace('"', "")

    content = request.get_json()

    excel = Excel(document)

    doc, res = excel.filter(str(sheet),  content)

    cols_values = []

    for i in doc.keys():
        cols_values.append([i, doc[i].unique()])

    table = doc.to_html()
    table = table.replace('<table border="1" class="dataframe">', '<table class="table table-bordered" id="dataTable" width="100%" cellspacing="0">')
    
    cols = ""
    if cols_values:
        for col in cols_values:
            val = ""
            for value in col[1]:
                if col[0] in content and str(value)==str(content[col[0]]):
                    val+='<option value="{0}" selected>{0}</option>'.format(value)
                else:
                    val+='<option value="{0}">{0}</option>'.format(value)
            cols += """
                <div class="col-md-2"> 
                <label for="{0}">{0}:</label>
                <select id="{0}" class="custom-select custom-select-sm form-control form-control-sm" form="search_form">
                    <option value="Seleccionar">Seleccionar</option>
                    {1}
                </select>
                </div>
            """.format(col[0], val)

    return {"table":table, "cols":cols}


@application.route('/load_database', methods=['POST'])
def load_database():
    if request.method == 'POST':
        f = request.files['file']
        f.save(os.path.join(UPLOAD_FOLDER, f.filename))
        upload_file(f"/tmp/{f.filename}", AWS_BUCKET_NAME)
        os.remove(f"/tmp/{f.filename}")
        excels[f.filename] = pd.read_excel("https://liverpoolexcel.s3-us-west-1.amazonaws.com//tmp/"+f.filename, None, dtype=str)
        excel = Excel(f.filename)
        excel.load_to_db()
    return home()

@application.route('/drop_database', methods=['GET'])
def drop_database():
    s3 = boto3.resource('s3')
    for i in documents_collection.find({}):
        s3.Object(AWS_BUCKET_NAME, i["name"]).delete()
    documents_collection.drop()
    excels = {'Seleccionar' : pd.read_excel("Seleccionar.xlsx", None)}
    return redirect("/")

if __name__ == "__main__":
    application.debug = True
    application.run()