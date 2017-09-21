var Message = require("./kafka");
var Log = require("./log");

var TOPIC = "test_测试";

var remotes = [
    {host: "JCloud-02", port: 7002},
    {host: "JCloud-02", port: 7102}
];

Message.startDaemon({
    remotes: remotes,
    debug: true,
    handlers: [{
        topic: TOPIC,
        batch_limit: 2,
        run: function (messages) {
            Log.d("收到消息:" + JSON.stringify(messages));
        }
    }]
}).then(function(isMaster) {
    if (isMaster) return;
    var client = new Message.MessageClient(remotes);
    sendMessages(client);
});

function sendMessages(client) {
    var messages = ["2", "4", "6", "7", "8"];
    Log.d("发送消息" + JSON.stringify(messages));
    client.publishMore(TOPIC, messages).then(function() {
        setTimeout(sendMessages.bind(null, client), 3000);
    });
}