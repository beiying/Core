var cluster = require("cluster");
var Q = require("q");

var Log = require("./log");
var Process = require("./process");
var Http = require("./http");

var RESTART_DELAY = 5000;

function startClusterServer(options) {
    return (cluster.isMaster) ? startMasterServer(options) : startWorkerServer(options);
}

function startMasterServer(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Process.handleExit(null, true);
    var deferred = Q.defer();
    setTimeout(createWorkers, 0);
    return deferred.promise;

    function createWorkers() {
        var numWorker = options.numWorker ? options.numWorker : 1;
        for (var i = 0; i < numWorker; ++i) {
            var worker = cluster.fork();
            Log.i("create worker process, pid = " + worker.process.pid);
        }
        cluster.on("exit", function(worker, code, signal) {
            Log.i("worker(pid=" + worker.process.pid + " exit with code=" + code + ", signal=" + signal + ", will restart after 5 seconds");
            setTimeout(function() {
                cluster.fork();
            }, RESTART_DELAY);
        });
        process.on("exit", function() {
            for (var id in cluster.workers) {
                cluster.workers[id].process.kill();
            }
        });
        deferred.resolve(true);
    }
}

function startWorkerServer(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);

    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
        options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
    ]).then(function () {
        return Http.startServer(options.port, options.serverApi);
    }).then(function (server) {
        Log.i("server start listening at port: " + options.port);
        Process.handleExit(server, false);
        return false;
    }).catch(function (err) {
        Log.fatal(name + " start failed, err=\n" + err.stack);
    });
}

function startClusterDaemon(options) {
    return (cluster.isMaster) ? startMasterDaemon(options) : startWorkerDaemon(options);
}

function startMasterDaemon(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Process.handleExit(null, true);
    var deferred = Q.defer();
    setTimeout(createWorker, 0);
    return deferred.promise;

    function createWorker() {
        var worker = cluster.fork();
        Log.i("create worker process, pid = " + worker.process.pid);
        cluster.on("exit", function(worker, code, signal) {
            Log.i("worker(pid=" + worker.process.pid + " exit with code=" + code + ", signal=" + signal + ", will restart after 5 seconds");
            setTimeout(function() {
                cluster.fork();
            }, RESTART_DELAY);
        });
        process.on("exit", function() {
            for (var id in cluster.workers) {
                cluster.workers[id].process.kill();
            }
        });
        deferred.resolve(true);
    }
}

function startWorkerDaemon(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Log.i(name, "worker deamon started. options =\n" + JSON.stringify(options, true, 2));
    Process.handleExit(null, false);
    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
        options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
    ]).then(function() {
        if ((options.tasks instanceof Array) && options.tasks.length > 0) {
            options.tasks.forEach(function(task) {
                if (typeof task.run == "function") {
                    doDaemon(task.run, task.timegap || 0);
                }
            });
        }
        return false;
    }).catch(function(err) {
        Log.fatal(name, "daemon start failed, err=\n" + err.stack);
    });
}

function doDaemon(run, timegap) {
    loop();

    function loop() {
        try {
            var ret = run();
            if (Q.isPromise(ret)) {
                ret.delay(timegap).then(loop).catch(function(err) {
                    Log.e("daemon run failed, err = \n" + err.stack);
                    loop();
                });
            } else {
                Q.delay(null, timegap).then(loop);
            }
        } catch(err) {
            Log.e("daemon run failed, err = \n" + err.stack);
            setTimeout(loop, timegap);
        }
    }
}

function isMaster() {
    return cluster.isMaster;
}

exports.startClusterServer = startClusterServer;
exports.startClusterDaemon = startClusterDaemon;
exports.isMaster = isMaster;