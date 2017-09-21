var Q = require("q");
var Http = require("./http");

var SCHEDULER_SERVER = "http://scheduler.api.jndroid.com";

function detail(looper, start, end, withTimestamp) {
    var url = SCHEDULER_SERVER + ((withTimestamp)? "/detail2" : "/detail") +"?looper=" + looper;
    if (start) url +="&start=" + start;
    if (end) url += "&end=" + end;
    return Http.request(url).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error(res.err_no);
        return res.result;
    });
}

function post(looper, event) {
    return Http.request(SCHEDULER_SERVER + "/post?looper=" + looper, "POST", event).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error(res.err_no);
    });
}

function postDelayed(looper, event, delay) {
    return Http.request(SCHEDULER_SERVER + "/postDelayed?looper=" + looper + "&delay=" + delay, "POST", event).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error(res.err_no);
    });
}

function get(looper) {
    return Http.request(SCHEDULER_SERVER + "/get?looper=" + looper).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error(res.err_no);
        return res.result;
    });
}

function getMore(looper, limit) {
    var url = SCHEDULER_SERVER + "/getMore?looper=" + looper;
    if (limit) url += "&limit=" + limit;
    return Http.request(url).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error(res.err_no);
        return res.result;
    });
}

exports.detail = detail;
exports.post = post;
exports.postDelayed = postDelayed;
exports.get = get;
exports.getMore = getMore;