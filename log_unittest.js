var assert = require("assert");
var Http = require("./http");
var Log = require("./log");

describe("test Log in develop mode", function() {

    it("log output format ok", function() {
        var output = "";
        Log.injectWriter({
            log: function(line) {
                output = line;
            }
        });
        Log.init("log_unittest", true);
        Log.debug("text1", "text2");
        assert.ok(output.indexOf("[DEBUG][Log]text1 text2") > 0);
        Log.info("text3");
        assert.ok(output.indexOf("[INFO][Log]text3") > 0);
        Log.error("text4");
        assert.ok(output.indexOf("[ERROR][Log]text4") > 0);
        var exitCode = 0;
        Log.injectExit(function(code) {
            exitCode = code;
        });
        Log.fatal("text5", "text6\n", "text7");
        assert.ok(output.indexOf("[FATAL][Log]text5 text6\n text7") > 0);
        assert.equal(exitCode, -1);
    });

});

describe("test Log in production mode", function() {
    this.timeout(30000);

    it("upload log ok", function(done) {
        Log.init("log_unittest");
        for (var i = 0; i < 10; ++i) {
            Log.debug("log message", i);
        }
        Log.injectExit(function(code) {
            assert.equal(code, -1);
            Http.request("http://logcat.api.jndroid.com/logs/get?module=log_unittest").then(function(data) {
                var logs = JSON.parse(data).result;
                assert.equal(logs.length, 11);
                return Http.request("http://logcat.api.jndroid.com/logs/del?module=log_unittest&from=0&to=" + (Date.now() + 10000));
            }).then(function() {
                done();
            }).catch(function(err) {
                done(err);
            });
        });
        Log.fatal("exit");
    });

});