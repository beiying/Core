var fs = require("fs");
var readline = require("readline");

var Q = require("q");
var Log = require("./log");
var Process = require("./process");
var File = require("./file");

var BUFFER_LIMIT = 16;
var WAIT_TIME = 50;

var mBufferedLines = null;
var mReadLineInterface = null;
var mDone = false;

function appendLine(filename, line) {
    return File.appendFile(filename, line + "\n", "utf-8");
}

function startDaemon(options) {
    var name = Process.getProcessName();
    Log.init(name);
    Log.i(name, "deamon started. options =\n" + JSON.stringify(options, true, 2));
    Process.handleExit(null, false);
    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
        options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
    ]).then(function() {
        return callSetup(options.setup);
    }).then(function() {
        mDone = false;
        mBufferedLines = [];
        mReadLineInterface = readline.createInterface({
            input: fs.createReadStream(options.filename, "utf-8")
        });
        mReadLineInterface.on("line", function(line) {
            if (line.length > 0) mBufferedLines.push(line);
            if (mBufferedLines.length > BUFFER_LIMIT) mReadLineInterface.pause();
        });
        mReadLineInterface.on("close", function() {
            mDone = true;
        });
        startRun(options.processLine, options.finish);
    }).catch(function(err) {
        Log.fatal(name, "daemon start failed, err=\n" + err.stack);
    });
}

function callSetup(setup) {
    if (typeof setup !== "function") {
        setup = function() {
            Log.e("文件处理开始...");
        };
    }
    return setup();
}

function startRun(processLine, finish) {
    if (mBufferedLines.length == 0) {
        if (mDone) {
            return callFinish(finish);
        } else {
            mReadLineInterface.resume();
            setTimeout(startRun.bind(null, processLine, finish), WAIT_TIME);
            return;
        }
    }
    var line = mBufferedLines.shift();
    var ret = processLine(line);
    if (Q.isPromise(ret)) {
        ret.then(startRun.bind(null, processLine, finish)).catch(function(err) {
            Log.e("发生异常, err=\n" + err.stack);
            startRun(processLine, finish);
        });
    } else {
        setTimeout(startRun.bind(null, processLine, finish), 0);
    }
}

function callFinish(finish) {
    if (typeof finish !== "function") {
        finish = function() {
            Log.e("文件处理结束");
            process.exit(0);
        };
    }
    var ret = finish();
    if (Q.isPromise(ret)) {
        return ret.catch(function(err) {
            Log.e("发生异常, err=\n" + err.stack);
        });
    }
}

exports.startDaemon = startDaemon;
exports.appendLine = appendLine;