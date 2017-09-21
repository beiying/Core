var Q = require("q");
var daemon_tools = require("./daemon");

daemon_tools.startDaemon({
    mysqlOption: null,
    redisOption: null
}, [{run: work1, timegap: 1000}, {run: work2, timegap: 3000}]);

function work1() {
    var deferred = Q.defer();
    setTimeout(function() {
        console.log("worker1 is working...");
        deferred.resolve();
    }, 10);
    return deferred.promise;
}

function work2() {
    var deferred = Q.defer();
    setTimeout(function() {
        console.log("worker2 is working...");
        if (Math.random() > 0.5) {
            deferred.resolve();
        } else {
            deferred.reject(new Error());
        }
    }, 10);
    return deferred.promise;
}