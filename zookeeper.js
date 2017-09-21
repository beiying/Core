var Q = require("q");
var zookeeper = require('node-zookeeper-client');
var Log = require("./log");

var ZOO_KEEPER_SERVER = [
    "JCloud-05:32181",
    "JCloud-05:32182",
    "JCloud-05:32183"
].join(",");

var mZooKeeperClient = null;

function connectZooKeeper() {
    var deferred = Q.defer();
    mZooKeeperClient = zookeeper.createClient(ZOO_KEEPER_SERVER);
    mZooKeeperClient.once("connected", function () {
        Log.d("ZooKeeper Server Connected.");
        deferred.resolve(mZooKeeperClient);
    });
    mZooKeeperClient.connect();
    return deferred.promise;
}

function close() {
    Log.d("Close ZooKeeper Server Connection.");
    mZooKeeperClient.close();
}

function create(path, data, acls, mode) {
    var deferred = Q.defer();
    mZooKeeperClient.create(path, data, acls, mode, function(err) {
        if (err) return deferred.reject(err);
        Log.d("Node:" + path + " is successfully created.");
        deferred.resolve();
    });
    return deferred.promise;
}

function mkdirp(path, data, acls, mode) {
    var deferred = Q.defer();
    mZooKeeperClient.mkdirp(path, data, acls, mode, function(err) {
        if (err) return deferred.reject(err);
        Log.d("Node:" + path + " is successfully created.");
        deferred.resolve();
    });
    return deferred.promise;
}

function remove(path, version) {
    var deferred = Q.defer();
    if (!version) version = -1;
    mZooKeeperClient.remove(path, version, function(err) {
        if (err) return deferred.reject(err);
        Log.d("Node:" + path + " is successfully deleted.");
        deferred.resolve();
    });
    return deferred.promise;
}

function exists(path, watcher) {
    var deferred = Q.defer();
    mZooKeeperClient.exists(path, watcher, function(err, stat) {
        if (err) return deferred.reject(err);
        return deferred.resolve(stat);
    });
    return deferred.promise;
}

function getChildren(path, watcher) {
    var deferred = Q.defer();
    mZooKeeperClient.getChildren(path, watcher, function (err, children, stats) {
        if (err) return deferred.reject(err);
        deferred.resolve(children);
    });
    return deferred.promise;
}

function getData(path, watcher) {
    var deferred = Q.defer();
    mZooKeeperClient.getData(path, watcher, function (err, data, stat) {
        if (err) return deferred.reject(err);
        deferred.resolve(data);
    });
    return deferred.promise;
}

function setData(path, data, version) {
    var deferred = Q.defer();
    if (!version) version = -1;
    mZooKeeperClient.setData(path, data, version, function (err, stat) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

function getState() {
    return mZooKeeperClient.getState();
}

function transaction() {
    return mZooKeeperClient.transaction();
}

function loadConfig(config, root, keys) {
    return connectZooKeeper().then(function() {
        var promises = keys.map(function(key) {
            return getData(root + "/" + key).then(function(data) {
                config[key] = JSON.parse(data.toString());
            });
        });
        return Q.all(promises);
    }).then(function() {
        Log.d("load configure done, config=\n" + JSON.stringify(config, true, 2));
        close();
    });
}

exports.connectZooKeeper = connectZooKeeper;
exports.close = close;
exports.create = create;
exports.mkdirp = mkdirp;
exports.remove = remove;
exports.exists = exists;
exports.getChildren = getChildren;
exports.getData = getData;
exports.setData = setData;
exports.getState = getState;
exports.transaction = transaction;
exports.loadConfig = loadConfig;