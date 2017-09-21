var Q = require("q");
var MongoClient = require("mongodb").MongoClient;
var ObjectID = require('mongodb').ObjectID;
var Log = require("./log");

function MongoDBClient(option) {

    var mOption = option;
    var mConnectedDB = null;

    this.connect = function() {
        var deferred = Q.defer();
        var url = "mongodb://";
        if (mOption.username) {
            url += mOption.username;
            if (mOption.password) {
                url += ":" + mOption.password;
            }
            url += "@";
        }
        url += mOption.host + ":" + mOption.port + "/" + mOption.database;
        MongoClient.connect(url, function(err, db) {
            if (err) return deferred.reject(err);
            mConnectedDB = db;
            mConnectedDB.on("close", function (err) {
                Log.e("MongoDB server is closed, err=\n" + err.stack);
            });
            deferred.resolve();
        });
        return deferred.promise;
    };

    this.insertOne = function(name, doc) {
        var deferred = Q.defer();
        mConnectedDB.collection(name).insertOne(doc, function(err, res) {
            if (err) return deferred.reject(err);
            deferred.resolve(res);
        });
        return deferred.promise;
    };

    this.updateOne = function(name, filter, update, options) {
        var deferred = Q.defer();
        mConnectedDB.collection(name).updateOne(filter, update, options, function(err, res) {
            if (err) return deferred.reject(err);
            deferred.resolve(res);
        });
        return deferred.promise;
    };

    this.deleteOne = function(name, filter) {
        var deferred = Q.defer();
        mConnectedDB.collection(name).deleteOne(filter, function(err, res) {
            if (err) return deferred.reject(err);
            deferred.resolve(res)
        });
        return deferred.promise;
    };

    this.deleteMany = function(name, filter) {
        var deferred = Q.defer();
        mConnectedDB.collection(name).deleteMany(filter, function(err, res) {
            if (err) return deferred.reject(err);
            deferred.resolve(res)
        });
        return deferred.promise;
    };

    this.findOne = function(name, query, fields, order) {
        var deferred = Q.defer();
        var cur = mConnectedDB.collection(name).find(query);
        if (fields) cur = cur.project(fields);
        if (order) cur = cur.sort(order);
        cur.limit(1).next(function(err, doc) {
            if (err) return deferred.reject(err);
            deferred.resolve(doc);
        });
        return deferred.promise;
    };

    this.distinct = function(name, key, query) {
        var deferred = Q.defer();
        mConnectedDB.collection(name).distinct(key, query, function(err, docs) {
            if (err) return deferred.reject(err);
            deferred.resolve(docs);
        });
        return deferred.promise;
    }
}

var connectedDB = null;

function connectMongoDB(options) {
    var deferred = Q.defer();
    var url = "mongodb://";
    if (options.username) {
        url += options.username;
        if (options.password) {
            url += ":" + options.password;
        }
        url += "@";
    }
    url += options.host + ":" + options.port + "/" + options.database;
    MongoClient.connect(url, function(err, db) {
        if (err) return deferred.reject(err);
        connectedDB = db;
        connectedDB.on("close", function (err) {
            Log.fatal("MongoDB server is closed, err=\n" + err.stack);
        });
        deferred.resolve();
    });
    return deferred.promise;
}

function close(force) {
    var deferred = Q.defer();
    connectedDB.removeAllListeners();
    connectedDB.close(force, function(err) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

function collection(name) {
    return connectedDB.collection(name);
}

function listCollections(filter) {
    var deferred = Q.defer();
    connectedDB.listCollections(filter).toArray(function(err, result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result);
    });
    return deferred.promise;
}

function dropCollection(name) {
    var deferred = Q.defer();
    connectedDB.dropCollection(name, function(err, result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result);
    });
    return deferred.promise;
}

function renameCollection(oldName, newName) {
    var deferred = Q.defer();
    connectedDB.collection(oldName).rename(newName, function(err, collection) {
        if (err) return deferred.reject(err);
        deferred.resolve(collection.collectionName);
    });
    return deferred.promise;
}

function insertOne(name, doc) {
    var deferred = Q.defer();
    connectedDB.collection(name).insertOne(doc, function(err, result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result);
    });
    return deferred.promise;
}

function insertMany(name, docs, options) {
    var deferred = Q.defer();
    connectedDB.collection(name).insertMany(docs, options, function(err, result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result);
    });
    return deferred.promise;
}

function updateOne(name, filter, update, options) {
    var deferred = Q.defer();
    connectedDB.collection(name).updateOne(filter, update, options, function(err, result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result);
    });
    return deferred.promise;
}

function updateMany(name, filter, update, options) {
    var deferred = Q.defer();
    connectedDB.collection(name).updateMany(filter, update, options, function(err, result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result);
    });
    return deferred.promise;
}


function count(name, query) {
    var deferred = Q.defer();
    connectedDB.collection(name).count(query, function(err, count) {
        if (err) return deferred.reject(err);
        deferred.resolve(count);
    });
    return deferred.promise;
}

function find(name, query, fields, order, limit, skip) {
    var deferred = Q.defer();
    var cur = connectedDB.collection(name).find(query);
    if (fields) cur = cur.project(fields);
    if (order) cur = cur.sort(order);
    if (limit !== undefined) cur = cur.limit(limit);
    if (skip !== undefined) cur = cur.skip(skip);
    cur.toArray(function(err, docs) {
        if (err) return deferred.reject(err);
        deferred.resolve(docs);
    });
    return deferred.promise;
}

function findOne(name, query, fields, order) {
    var deferred = Q.defer();
    var cur = connectedDB.collection(name).find(query);
    if (fields) cur = cur.project(fields);
    if (order) cur = cur.sort(order);
    cur.limit(1).next(function(err, doc) {
        if (err) return deferred.reject(err);
        deferred.resolve(doc);
    });
    return deferred.promise;
}

function findOneAndUpdate(name, filter, update, options) {
    var deferred = Q.defer();
    connectedDB.collection(name).findOneAndUpdate(filter, update, options, function(err, result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result.value);
    });
    return deferred.promise;
}

function deleteOne(name, filter) {
    var deferred = Q.defer();
    connectedDB.collection(name).deleteOne(filter, function(err,result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result)
    });
    return deferred.promise;
}

function deleteMany(name, filter) {
    var deferred = Q.defer();
    connectedDB.collection(name).deleteMany(filter, function(err,result) {
        if (err) return deferred.reject(err);
        deferred.resolve(result)
    });
    return deferred.promise;
}

function distinct(name, key, query) {
    var deferred = Q.defer();
    connectedDB.collection(name).distinct(key, query, function(err, docs) {
        if (err) return deferred.reject(err);
        deferred.resolve(docs);
    });
    return deferred.promise;
}

function registerEvent(eventName, callBack) {
    var deferred = Q.defer();
    connectedDB.on(eventName, callBack);
    deferred.resolve(true);
    return deferred.promise;
}

function unregisterEvent() {
    var deferred = Q.defer();
    connectedDB.removeAllListeners();
    deferred.resolve(true);
    return deferred.promise;
}

exports.MongoDBClient = MongoDBClient;

exports.ObjectID = ObjectID;
exports.connectMongoDB = connectMongoDB;
exports.close = close;
exports.collection = collection;
exports.listCollections = listCollections;
exports.dropCollection = dropCollection;
exports.renameCollection = renameCollection;
exports.insertOne = insertOne;
exports.insertMany = insertMany;
exports.updateOne = updateOne;
exports.updateMany = updateMany;
exports.count = count;
exports.find = find;
exports.findOne = findOne;
exports.findOneAndUpdate = findOneAndUpdate;
exports.deleteOne = deleteOne;
exports.deleteMany = deleteMany;
exports.distinct = distinct;
exports.registerEvent = registerEvent;
exports.unregisterEvent = unregisterEvent;