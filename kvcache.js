var Q = require("q");
var Http = require("./http");

var KVCACHE_SET = "http://kvcache.api.jndroid.com/set?key=";
var KVCACHE_GET = "http://kvcache.api.jndroid.com/get?key=";

function set(key, value) {
    return Http.request(KVCACHE_SET + key, "POST", value).then(function(data) {
        if (data != "OK") throw new Error(data);
        return data;
    });
}

function get(key) {
    return Http.request(KVCACHE_GET + key).then(function(data) {
        return data;
    });
}

exports.get = get;
exports.set = set;