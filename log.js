var SEVERITY_DEBUG = 0;
var SEVERITY_INFO = 1;
var SEVERITY_ERROR = 2;
var SEVERITY_FATAL = 3;
var SEVERITIES = ["DEBUG", "INFO", "ERROR", "FATAL"];

var mModule = null;

function init(module) {
    mModule = module;
}

function debug() {
    console.log(formatDevLog(SEVERITY_DEBUG, arguments));
}

function info() {
    console.log(formatDevLog(SEVERITY_INFO, arguments));
}

function error() {
    console.log(formatDevLog(SEVERITY_ERROR, arguments));
}

function fatal() {
    console.log(formatDevLog(SEVERITY_FATAL, arguments));
    process.exit(-1);
}

function formatDevLog(severityLevel, logArgs) {
    return "[" + new Date().toString() + "][" + SEVERITIES[severityLevel] + "][" + mModule + "]" + Array.prototype.join.call(logArgs, " ");
}

exports.init = init;
exports.d = exports.debug = debug;
exports.i = exports.info = info;
exports.e = exports.error = error;
exports.fatal = fatal;