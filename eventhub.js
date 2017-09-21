var cluster = require("cluster");
var os = require("os");
var Q = require("q");
var io = require('socket.io-client');

var Log = require("./log");
var Process = require("./process");
var Http = require("./http");

var EVENTHUB_SERVER = "http://eventhub.api.jndroid.com";
var RESTART_DELAY = 5000;

var mSocket = null;

function connect() {
    var deferred = Q.defer();
    mSocket = io.connect(EVENTHUB_SERVER);
    mSocket.on("connect", function () {
        deferred.resolve();
    });
    return deferred.promise;
}

function addEventHandler(event, handler) {
    if (mSocket) {
        mSocket.on(event, handler);
    }
}

function emit(event, msg) {
    if (mSocket) {
        mSocket.emit(event, msg);
    }
}

function startClusterEventHub(options) {
    return (cluster.isMaster) ? startMasterEventHub(options) : startWorkerEventHub(options);
}

function startMasterEventHub(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Process.handleExit(null, true);
    var deferred = Q.defer();
    setTimeout(createWorkers, 0);
    return deferred.promise;

    function createWorkers() {
        var numWorker = options.numWorker ? options.numWorker : os.cpus().length;
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

function startWorkerEventHub(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
        options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
    ]).then(function(results) {
        exports.mysqlClient = results[0];
        exports.redisClient = results[1];
        var eventHubSocket = io.connect(EVENTHUB_SERVER);
        eventHubSocket.on("connect", function () {
            options.eventHandlers.forEach(function(eventHandler) {
                Log.i("start accepting event: " + eventHandler.accept_event);
                eventHubSocket.on(eventHandler.accept_event, function (msg) {
                    eventHandler.handler(msg).then(function(result) {
                        eventHubSocket.emit(eventHandler.response_event, result);
                    });
                });
            });
        });
        return false;
    }).catch(function(err) {
        Log.fatal(name, "daemon start failed, err=\n" + err.stack);
    });
}

exports.connect = connect;
exports.addEventHandler = addEventHandler;
exports.emit = emit;
exports.startClusterEventHub = startClusterEventHub;