var util = require("./util");

console.log("result = ", util.decodeURIComponentSafely("%e4%bd%a0%e5%a5%bd"));
console.log("result = ", util.decodeURIComponentSafely("%e4%a0%e5%a5%bd"));
console.log("result = ", util.parseJsonSafely('{"err":-1}'));
console.log("result = ", util.parseJsonSafely('}{'));


var synchronized = require("./synchronized_tools").synchronized;

function foo(arg1, arg2, done) {
    console.log("step 1 ", arg1, arg2);
    setTimeout(function(){
        console.log("step 2 ", arg1, arg2);
        setTimeout(function() {
            console.log("step 3 ", arg1, arg2);
            setTimeout(done, 1000);
        }, 1000);
    }, 1000);
}

synchronized(function(done) {
    foo('a', 'A', done);
});
synchronized(function(done) {
    foo('b', 'B', done);
});
setTimeout(function() {
    synchronized(function(done) {
        foo('c', 'C', done);
    });
}, 200);
