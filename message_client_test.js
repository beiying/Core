var Message = require("./kafka");
var Log = require("./log");

var TOPIC = "topic_测试";

var remotes = [
    {host: "JCloud-02", port: 7002},
    {host: "JCloud-02", port: 7102}
];

var mClient = new Message.MessageClient(remotes);

return mClient.publish(TOPIC, "哈哈").then(function() {
    return mClient.publishMore(TOPIC, ["呵呵", "呼啦啦", "西瓜"]);
}).then(function() {
    return mClient.detail(TOPIC, 0, -1);
}).then(function(messages) {
    Log.i("detail:" + JSON.stringify(messages));
    return mClient.trySubscribe(TOPIC);
}).then(function(message) {
    Log.i("收到:" + message);
    return mClient.subscribeMore(TOPIC, 2);
}).then(function(messages) {
    Log.i("收到:" + JSON.stringify(messages));
    return mClient.trySubscribeMore(TOPIC, 0);
}).then(function(messages) {
    Log.i("收到:" + JSON.stringify(messages));
}).catch(function(err) {
    Log.d("出错了！err=" + err.stack);
});