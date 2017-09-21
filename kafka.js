var cluster = require("cluster");
var kafka = require("kafka-node");
var Q = require("q");
var Log = require("./log");
var Process = require("./process");

const KAFKA_BROKERS = [
    "JCloud-05:9092",
    "JCloud-05:9093",
    "JCloud-05:9094"
];

const RESTART_DELAY = 5000;
const BUFFER_LIMIT = 16;
const WAIT_TIME = 50;

var mKafkaClient = null;
var mProducer = null;
var mConsumer = null;

var mBufferedMessages = null;

function initProducer() {
    var deferred = Q.defer();
    initProducerInternal();
    return deferred.promise;

    function initProducerInternal() {
        mKafkaClient = new kafka.KafkaClient({
            kafkaHost: KAFKA_BROKERS.join(",")
        });
        mProducer = new kafka.HighLevelProducer(mKafkaClient);
        mProducer.on("ready", function() {
            deferred.resolve();
        });
        mProducer.on("error", function(err) {
            Log.e("Producer Error: " + err.stack);
            setTimeout(initProducerInternal, 1000);
        });
    }
}

function publish(topics, messages) {
    if (!(topics instanceof Array)) topics = [topics];
    var payloads = topics.map(function(topic) {
        return {
            topic: topic,
            messages: messages
        };
    });
    var deferred = Q.defer();
    mProducer.send(payloads, function(err, data) {
        if (err) return deferred.reject(err);
        deferred.resolve(data);
    });
    return deferred.promise;
}

function startConsumer(options) {
    return (cluster.isMaster) ? startMasterConsumer(options) : startWorkerConsumer(options);
}

function startMasterConsumer(options) {
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

function startWorkerConsumer(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Log.i(name, "worker deamon started. options =\n" + JSON.stringify(options, true, 2));
    Process.handleExit(null, false);
    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
        options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
    ]).then(function() {
        mKafkaClient = new kafka.KafkaClient({
            kafkaHost: KAFKA_BROKERS.join(",")
        });
        var payloads = options.topics.map(function(topic) {
            return {topic: topic};
        });
        mConsumer = new kafka.Consumer(mKafkaClient, payloads);
        mBufferedMessages = [];
        mConsumer.on("message", function(data) {
            mBufferedMessages.push(data);
            if (mBufferedMessages.length > BUFFER_LIMIT) {
                mConsumer.pause();
            }
        });
        startRun(options.handler);
        return false;
    }).catch(function(err) {
        Log.fatal(name, "worker start failed, err=\n" + err.stack);
    });
}

function startRun(handler) {
    if (mBufferedMessages.length === 0) {
        mConsumer.resume();
        setTimeout(startRun.bind(null, handler), WAIT_TIME);
        return;
    }
    var data = mBufferedMessages.shift();
    console.log(mBufferedMessages.length);
    var ret = handler(data.topic, data.value);
    if (Q.isPromise(ret)) {
        ret.then(startRun.bind(null, handler)).catch(function(err) {
            Log.e("handle messages failed, err = \n" + err.stack);
            startRun(handler);
        });
    } else {
        setTimeout(startRun.bind(null, handler), 0);
    }
}

function isMaster() {
    return cluster.isMaster;
}

exports.initProducer = initProducer;
exports.publish = publish;
exports.startConsumer = startConsumer;
exports.isMaster = isMaster;