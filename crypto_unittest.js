var assert = require("assert");
var Crypto = require("./crypto");

describe("Crypto", function() {

    describe("md5sum", function() {
        it("string hash ok", function() {
            assert.equal(Crypto.md5sum("hello, world!"), "3adbbad1791fbae3ec908894c4963870");
            assert.equal(Crypto.md5sum("hello, world!", "base64"), "Otu60XkfuuPskIiUxJY4cA==");
        });
    });

    describe("sha1sum", function() {
        it("string hash ok", function() {
            assert.equal(Crypto.sha1sum("hello, world!"), "1f09d30c707d53f3d16c530dd73d70a6ce7596a9");
            assert.equal(Crypto.sha1sum("hello, world!", "base64"), "HwnTDHB9U/PRbFMN1z1wps51lqk=");
        });
    });

});