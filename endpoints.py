from flask_pymongo import pymongo
from flask import request
from dotenv import load_dotenv
import os

from ray import method
from sklearn import metrics
# from sparkAnalysis import eda

load_dotenv()
client = pymongo.MongoClient(os.getenv("MONGO_STRING"))
db = client.get_database('ml-data-analytics')
userCollection = pymongo.collection.Collection(db, 'users')
print("MongoDB connected successfully")


def project_api_routes(endpoints):

    @endpoints.route('/signup', methods=['POST'])
    def signup():
        resp = {}
        try:
            req_body = request.json
            user = userCollection.find_one({"email": req_body['email']})
            if user is None:
                userCollection.insert_one(req_body)
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
                        "email": user['email'],
                        "org": user['org']
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
            # print(metrics)
            status = {
                "code":200,
                "filename": file.filename,
                "length": file.content_length,
                "type": file.content_type,
                "metrics": metrics
            }
        except Exception as e:
            print(e)
            status = {
                "code":500,
                "message": str(e)
            }
        resp["status"] = status
        return resp
    
    @endpoints.route('/algorithm-analysis',methods=['POST'])
    def algorithm_analysis():
        resp = {}
        try:
            file = request.files.get('file')
            # print(file)
            features = request.form.get('features')
            # print(features)
            target = request.form.get('target')
            # print(target)
            status = {
                "code":200,
                "filename": file.filename,
                "length": file.content_length,
                "type": file.content_type,
                "features": features,
                "target": target
            }
        except Exception as e:
            print(e)
            status = {
                "code":500,
                "message": str(e)
            }
        resp["status"] = status
        return resp

    # @endpoints.route('/upload', methods=['POST'])
    # def upload():
    #     resp = {}
    #     try:
    #         # req_body = request.files['']
    #         file = request.files['file']
    #         print(file)
    #         status = {
    #             "code": 200,
    #             "file": file.filename,
    #             "length": file.content_length,
    #             "type": file.content_type,
    #             "analysis": eda(file)
    #         }
    #     except Exception as e:
    #         print(e)
    #         status = {
    #             "code": "500",
    #             "message": str(e)
    #         }
    #     resp["status"] = status
    #     return resp

    return endpoints
