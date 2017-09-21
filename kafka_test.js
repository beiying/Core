var Q = require("q");
var Kafka = require("./kafka");
var Log = require("./log");

Kafka.startConsumer({
    topics: ["lego_to_lego_engine"],
    handler: processMessage
}).catch(function(err) {
    Log.e(err.stack);
});

function processMessage(topic, message) {
    return Q.fcall(function () {
        Log.d(topic, message);
        return Q.delay(null, 1000);
    });
}