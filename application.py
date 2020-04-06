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

class Excel:
    def __init__(self, name):
        self.name = name
        try:
            self.df = pd.read_excel("https://liverpoolexcel.s3-us-west-1.amazonaws.com//tmp/"+name, None)
        except:
            self.df = pd.read_excel("Seleccionar.xlsx", None)
        finally:
            self.sheets = self.df.keys()

    def get_sheets(self):
        return self.df.keys()

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
                if filters[i].isnumeric():
                    df =  df.loc[df[i] == int(filters[i])]
                else: 
                    df =  df.loc[df[i] == filters[i]]
            return df, 200
        return df, 500

def upload_file(file_name, bucket):
    object_name = file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
    return response

@application.route("/", methods=['GET'])
def home():
    return render_template("index.html")


@application.route("/tables/<document>/<sheet>", methods=['GET', 'POST'])
def tables(document, sheet):

    documents = documents_collection.find({})
    documents2 = documents_collection.find({})

    document_db = documents_collection.find_one({"name":document})
    

    if document_db==None or len(document_db)<1:
        excel = Excel(document)
        sheets = excel.get_sheets()
    else:
        sheets = list(document_db["sheets"].values())
        excel = Excel(document_db["name"])
        if sheet == "Seleccionar":
            sheet = sheets[0]
    
    doc, res = excel.filter(sheet,  1)

    del excel

    cols_values = []

    for i in doc.keys():
        cols_values.append([i, doc[i].unique()])

    table = doc[:100].to_html()
    table = table.replace('<table border="1" class="dataframe">', '<table class="table table-bordered" id="dataTable" width="100%" cellspacing="0">')
    
    return render_template('tables.html',  
                            table=table,
                            documents=documents,
                            documents2=documents2,
                            sheets=sheets,
                            document_selected=document,
                            sheet_selected=sheet,
                            cols_values=cols_values)

@application.route("/get_pages/<document>", methods=['GET', 'POST'])
def get_pages(document):
    document = documents_collection.find_one({"name":document})
    return Response(dumps(document["sheets"]), mimetype='application/json')

@application.route("/tables_filter/<document>/<sheet>", methods=['GET', 'POST'])
def tables_filter(document, sheet):
    content = request.get_json()
    document_db = documents_collection.find_one({"name":document})

    excel = Excel(document_db["name"])
    doc, res = excel.filter(str(sheet),  content)

    cols_values = []

    for i in doc.keys():
        cols_values.append([i, doc[i].unique()])

    table = doc[:100].to_html()
    table = table.replace('<table border="1" class="dataframe">', '<table class="table table-bordered" id="dataTable" width="100%" cellspacing="0">')
    
    return table  


@application.route('/load_database', methods=['POST'])
def load_database():
    if request.method == 'POST':
        f = request.files['file']
        f.save(os.path.join(UPLOAD_FOLDER, f.filename))
        upload_file(f"/tmp/{f.filename}", AWS_BUCKET_NAME)
        os.remove(f"/tmp/{f.filename}")
        excel = Excel(f.filename)
        excel.load_to_db()
    return redirect("/tables/Seleccionar.xlsx/Seleccionar")

@application.route('/drop_database', methods=['GET'])
def drop_database():
    s3 = boto3.resource('s3')
    for i in documents_collection.find({}):
        s3.Object(AWS_BUCKET_NAME, i["name"]).delete()
    documents_collection.drop()
    return redirect("/tables/Seleccionar.xlsx/Seleccionar")

if __name__ == "__main__":
    application.debug = True
    application.run()