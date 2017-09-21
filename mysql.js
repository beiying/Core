var mysql = require("mysql");
var Q = require("q");

var mysqlClient = null;

function connectMySql(options) {
    var deferred = Q.defer();
    mysqlClient = mysql.createConnection(options);
    mysqlClient.connect(function(err) {
        if (err) return deferred.reject(err);
        exports.client = mysqlClient;
        deferred.resolve(mysqlClient);
    });
    return deferred.promise;
}

function query(sqlString, values) {
    var deferred = Q.defer();
    mysqlClient.query(sqlString, values, function(err, results) {
        if (err) return deferred.reject(err);
        deferred.resolve(results);
    });
    return deferred.promise;
}

exports.connectMySql = connectMySql;
exports.query = query;