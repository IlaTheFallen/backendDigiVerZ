from fileinput import filename
from flask_pymongo import pymongo
from flask import request
from dotenv import load_dotenv
from bson.objectid import ObjectId
import os
import gridfs
from datetime import datetime
# from sparkAnalysis import eda
from algorithmAnalyser import regression 

load_dotenv()
client = pymongo.MongoClient(os.getenv("MONGO_STRING"))
db = client.get_database('ml-data-analytics')
fs = gridfs.GridFS(db)
# db = client['ml-data-analytics']
userCollection = pymongo.collection.Collection(db, 'users')
# userCollection = db['users']
# print(userCollection.count_documents({}))
print("MongoDB connected successfully")


def project_api_routes(endpoints):

    @endpoints.route('/signup', methods=['POST'])
    def signup():
        resp = {}
        try:
            req_body = request.json
            user = userCollection.find_one({"email": req_body['email']})
            if user is None:
                data = {
                    "_id": userCollection.count_documents({}) + 1,
                    "name": req_body['name'],
                    "email": req_body["email"],
                    "org": req_body["org"],
                    "password": req_body['password'],
                    "data-analysis": [],
                    "model-builder": [],
                    "sales-forecast": [],
                    "algorithm-analyzer": []
                }
                userCollection.insert_one(data)
                print("User added.")
                status = {
                    "code": 200,
                    "key": 1,
                    "message": "User added to database."
                }
            else:
                print("User already found.")
                status = {
                    "code": 200,
                    "key": 0,
                    "message": "User already found."
                }
        except Exception as e:
            print(e)
            status = {
                "code": 500,
                "message": str(e)
            }
        resp["status"] = status
        return resp

    @endpoints.route('/signin', methods=['GET'])
    def signin():
        resp = {}
        try:
            email = request.args.get('email')
            password = request.args.get('password')
            user = userCollection.find_one({"email": email})
            if user is None:
                print("Invalid username.")
                status = {
                    "code": 200,
                    "key": 0,
                    "message": "Invalid username"
                }
            else:
                if password == user['password']:
                    print('Login Success.')
                    resp["user"] = {
                        "_id": user['_id'],
                        "name": user['name'],
                        "email": user['email'],
                        "org": user['org'],
                        "data-analysis": user['data-analysis'],
                        "model-builder": user['model-builder'],
                        "sales-forecast": user['sales-forecast'],
                        "algorithm-analyzer": user['algorithm-analyzer']
                    }
                    status = {
                        "code": 200,
                        "key": 1,
                        "message": "Login successful!"
                    }
                else:
                    print("Wrong Password")
                    status = {
                        "code": 200,
                        "key": 2,
                        "message": "Wrong password"
                    }
        except Exception as e:
            print(e)
            status = {
                "code": 500,
                "message": str(e)
            }
        resp["status"] = status
        return resp

    @endpoints.route('/eda-analysis', methods=['POST'])
    def eda_analysis():
        resp = {}
        try:
            file = request.files.get("file")
            # print(file)
            metrics = request.form.get("metrics")
            id = request.form.get("id")
            # print(metrics)
            status = {
                "code": 200,
                "filename": file.filename,
                "length": file.content_length,
                "type": file.content_type,
                "metrics": metrics,
                "datetime": datetime.now(),
                # "analysis": eda(file)
            }
            userCollection.update_one(
            {"_id": int(id)},
            {"$push": {
                "data-analysis": status
            }}
        )
        except Exception as e:
            print(e)
            status = {
                "code": 500,
                "message": str(e)
            }
        resp["status"] = status
        return resp
    
    @endpoints.route('/eda-history', methods=['POST'])
    def eda_history():
        resp = {}
        try:
            req_body = request.json
            user = userCollection.find_one({"_id": int(req_body['id'])})
            resp['history'] = user['data-analysis']
        except Exception as e:
            print(e)
            status = {
                "code":500,
                "message": str(e)
            }
            resp['status'] = status
        return resp

    @endpoints.route('/algorithm-analysis', methods=['POST'])
    def algorithm_analysis():
        resp = {}
        try:
            file = request.files.get('file')
            # print(file)
            # features = request.form.get('features')
            # print(features)
            target = request.form.get('target')
            id = request.form.get("id")
            # print(target)
            status = {
                "code": 200,
                "filename": file.filename,
                "length": file.content_length,
                "type": file.content_type,
                # "features": features,
                "target": target,
                "datetime": datetime.now(),
                "analysis": regression(file,target)
            }
            userCollection.update_one(
            {"_id": int(id)},
            {"$push": {
                "algorithm-analyzer": status
            }}
            )
        except Exception as e:
            print(e)
            status = {
                "code": 500,
                "message": str(e)
            }
        resp["status"] = status
        return resp

    @endpoints.route('/algorithm-analysis-history', methods=['POST'])
    def algorithm_analysis_history():
        resp = {}
        try:
            req_body = request.json
            user = userCollection.find_one({"_id": int(req_body['id'])})
            resp['history'] = user['algorithm-analyzer']
        except Exception as e:
            print(e)
            status = {
                "code":500,
                "message": str(e)
            }
            resp['status'] = status
        return resp

    @endpoints.route('/upload', methods=['POST'])
    def upload():
        resp = {}
        try:
            # req_body = request.files['']
            file = request.files['file']
            print(file.filename)
            # client.save_file(file.filename,file)
            data = fs.put(file,filename=file.filename)
            print(ObjectId(data))
            # print(file)
            status = {
                "code": 200,
                "file": file.filename,
                # "length": file.content_length,
                "type": file.content_type,
                # "analysis": eda(file)
            }
        except Exception as e:
            print(e)
            status = {
                "code": "500",
                "message": str(e)
            }
        resp["status"] = status
        return resp

    return endpoints
