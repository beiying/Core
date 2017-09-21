var assert = require("assert");
var Q = require("q");
var File = require("./file");

describe("File", function() {
    describe("file operation", function() {
        it("writeFile & readFile & unlink OK", function(done) {
            File.writeFile("test.txt", "hello, world!").then(function() {
                return File.readFile("test.txt", "utf-8");
            }).then(function(data) {
                assert.equal(data, "hello, world!");
                return File.unlink("test.txt");
            }).then(function() {
                done();
            }).catch(function(err) {
                done(err);
            });
        });
    });

    describe("directory operation", function() {
        it("mkdir & rmdir OK", function(done) {
            File.mkdir("./test").then(function() {
                return File.rmdir("./test");
            }).then(function() {
                done();
            }).catch(function(err) {
                done(err);
            });
        });
        it("mkdirs & readdir & rmdirs OK", function(done) {
            File.mkdirs("a/b/c/").then(function() {
                return Q.all([
                    File.writeFile("a/b/c/file1.txt", "data1"),
                    File.writeFile("a/b/c/file2.txt", "data2"),
                    File.writeFile("a/b/file.txt", "data")
                ]);
            }).then(function() {
                return File.readdir("a/b/c");
            }).then(function(files) {
                assert.equal(files.length, 2);
                assert.equal(files[0], "file1.txt");
                assert.equal(files[1], "file2.txt");
                return File.rmdirs("a/");
            }).then(function() {
                done();
            }).catch(function(err) {
                done(err);
            });
        });
    });
});