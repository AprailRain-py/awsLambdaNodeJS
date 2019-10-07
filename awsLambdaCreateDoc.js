'use strict'
const AWS = require('aws-sdk');
var MongoClient = require('mongodb').MongoClient;

let atlas_connection_uri;
let cachedDb = null;

exports.handler = (event, context, callback) => {
    //the following line is critical for performance reasons to allow re-use of database connections across calls to this Lambda function and avoid closing the database connection. The first call to this lambda function takes about 5 seconds to complete, while subsequent, close calls will only take a few hundred milliseconds.
    // context.callbackWaitsForEmptyEventLoop = false;

    // const done = (err, res) => callback(null, {
    //     statusCode: err ? '400' : '200',
    //     body: err ? err.message : res,
    //     headers: {
    //         'Content-Type': 'application/json',
    //     },
    // });

    var uri = process.env['MONGODB_ATLAS_CLUSTER_URI'];

    if (atlas_connection_uri != null) {
        processEvent(event, context, callback);
    }
    else {
        const kms = new AWS.KMS();
        kms.decrypt({ CiphertextBlob: new Buffer(uri, 'base64') }, (err, data) => {
            if (err) {
                console.log('Decrypt error:', err);
                return callback(err);
            }
            atlas_connection_uri = data.Plaintext.toString('ascii');
            processEvent(event, context, callback);
        });
    }
};

function processEvent(event, context, callback) {
    console.log('Calling MongoDB Atlas from AWS Lambda with event: ' + JSON.stringify(event));
    var jsonContents = JSON.parse(JSON.stringify(event));
    
    context.callbackWaitsForEmptyEventLoop = false;
 
    try {
        if (cachedDb == null) {
            console.log('=> connecting to database');
            MongoClient.connect(atlas_connection_uri, { useNewUrlParser: true },function (err, db) {
                db = db.db('taskIN');
                cachedDb = db;
                return createDoc(db, jsonContents, callback);
            });
        }
        else {
            createDoc(cachedDb, jsonContents, callback);
        }
    }
    catch (err) {
        console.error('an error occurred', err);
    }
}

function createDoc(db, json, callback) {
    
    switch (json.httpMethod){
        case 'POST':
            db.collection('task').insertOne(json.body, function (err, result) {
                if (err != null) {
                    console.error("an error occurred in createDoc", err);
                    callback(null, JSON.stringify(err));
                }
                else {
                    // result;
                    console.log("Done: " + result.insertedId);
                    callback(null, "Success");
                }
            });
            break;
            
        case 'GET':
            
            switch (json.key){
                case 'status':
                    var objID = require('mongodb').ObjectID(json.body._id);
                    db.collection('task').findOne({_id:objID},function (err, result) {
                        if (err != null) {
                            callback(null, JSON.stringify(err));
                        }
                        else {
                            callback(null, result.statusHistory);
                        }
                    });
                    break;
                case 'SM':
                    db.collection('task').find(json.body).toArray(function (err, result) {
                        if (err != null) {
                            callback(null, JSON.stringify(err));
                        }
                        else {
                            callback(null, result);
                        }
                    });
                    break;
        default:
            console.log('Unsupported HTTP Method');
            }
    }
    
        //we don't need to close the connection thanks to context.callbackWaitsForEmptyEventLoop = false (above)
        //this will let our function re-use the connection on the next called (if it can re-use the same Lambda container)
        //db.close();
   
}