/* jshint esnext: true */
require('events').EventEmitter.prototype._maxListeners = 0;
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-creator-c/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-creator-c/lib/email_lib.js');
var functions = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-creator-c/lib/func_lib.js');
var fs = require("fs");
var express = require("express");
var http = require('http');
var https = require('https');
var options = {
    key: fs.readFileSync(config.security.key),
    cert: fs.readFileSync(config.security.cert)
};
var app = express();
var bodyParser = require("body-parser");
var cors = require("cors");
var amqp = require('amqplib/callback_api');
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static("./public"));
app.use(cors());

app.use(function(req, res, next) {
    next();
});

var server = https.createServer(options, app).listen(config.port.creator_c_port, function() {
    email.sendNewApiCreatorCIsUpEmail();
});


/**
 *   RabbitMQ connection object
 */
var amqpConn = null;


/**
 *  Subscribe user on topic to receive messages
 */
function subscribeToTopic(topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiCreatorC.*';
            var toipcName = `apiCreatorC.${topic}`;
            
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.assertQueue(toipcName, { exclusive: false, auto_delete: true }, function(err, q) {
                ch.bindQueue(q.queue, exchange, toipcName);
                ch.consume(q.queue, function(msg) {
                    var message = JSON.parse(msg.content.toString());
                    if (toipcName === `apiCreatorC.${config.rabbitmq.topics.saveCreatorTextPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.saveCreatorPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.updateCreatorPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.likeCreatorPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.unlikeCreatorPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.postCommentForCreatorPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.deleteCreatorPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.connectWithCreatorQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.disconnectWithCreatorQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.uploadVideoPostQ}`){

                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.newCreatorU}`){
                        functions.saveNewCreator(message, amqpConn, config.rabbitmq.topics.newCreator);
                    } else if (toipcName === `apiCreatorC.${config.rabbitmq.topics.newCreatorQ}`){
                        
                    }
                }, { noAck: true });
            });
        });
    }
}

/**
 *  Connect to RabbitMQ
 */
function connectToRabbitMQ() {
    amqp.connect(config.rabbitmq.url, function(err, conn) {
        if (err) {
            console.error("[AMQP]", err.message);
            return setTimeout(connectToRabbitMQ, 1000);
        }
        conn.on("error", function(err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
            }
        });
        conn.on("close", function() {
            console.error("[AMQP] reconnecting");
            return setTimeout(connectToRabbitMQ, 1000);
        });
        console.log("[AMQP] connected");
        amqpConn = conn;

        subscribeToTopic(config.rabbitmq.topics.saveCreatorTextPostQ);
        subscribeToTopic(config.rabbitmq.topics.saveCreatorPostQ);
        subscribeToTopic(config.rabbitmq.topics.updateCreatorPostQ);
        subscribeToTopic(config.rabbitmq.topics.likeCreatorPostQ);
        subscribeToTopic(config.rabbitmq.topics.unlikeCreatorPostQ);
        subscribeToTopic(config.rabbitmq.topics.postCommentForCreatorPostQ);
        subscribeToTopic(config.rabbitmq.topics.deleteCreatorPostQ);
        subscribeToTopic(config.rabbitmq.topics.connectWithCreatorQ);
        subscribeToTopic(config.rabbitmq.topics.disconnectWithCreatorQ);
        subscribeToTopic(config.rabbitmq.topics.uploadVideoPostQ);
        subscribeToTopic(config.rabbitmq.topics.newCreatorU);
    });
}
connectToRabbitMQ();


/**
 *  SOCKET.IO listeners
 */
var io = require("socket.io")(server, { transports: ['websocket'] });
io.sockets.on("connection", function(socket) {
    /**
     * on.saveCreatorPost listens for new posts
     */
    socket.on("saveCreatorTextPost", function(userName, postCapture, postType, creatorProfilePic, postText, postUUID) {
        functions.saveCreatorTextPost(userName, postCapture, postType, creatorProfilePic, postText, postUUID, socket, amqpConn, config.rabbitmq.topics.saveCreatorTextPost);
    });
    
    /**
     * on.saveCreatorPost listens for new posts
     */
    socket.on("saveCreatorPost", function(userName, postCapture, postType, creatorProfilePic, post, postUUID) {
        functions.saveCreatorPost(userName, postCapture, postType, creatorProfilePic, post, postUUID, socket, amqpConn, config.rabbitmq.topics.saveCreatorPost);
    });

    /**
     * on.updateCreatorPost listens for post updates
     */
    socket.on("updateCreatorPost", function(userName, postCapture, creatorProfilePic, post, postUUID) {
        functions.updateCreatorPost(userName, postCapture, creatorProfilePic, post, postUUID, socket, amqpConn, config.rabbitmq.topics.updateCreatorPost);
    });

    /**
     * on.likeCreatorPost listens for post likes
     */
    socket.on("likeCreatorPost", function(userName, postUUID, userProfilePicUrl) {
        functions.likeCreatorPost(userName, postUUID, userProfilePicUrl, socket, amqpConn, config.rabbitmq.topics.likeCreatorPost);
    });

    /**
     * on.unlikeCreatorPost listens for post unlikes
     */
    socket.on("unlikeCreatorPost", function(userName, postUUID, userProfilePicUrl) {
        functions.unlikeCreatorPost(userName, postUUID, userProfilePicUrl, socket, amqpConn, config.rabbitmq.topics.unlikeCreatorPost);
    });

    /**
     * on.postCommentForCreatorPost listens for post comments
     */
    socket.on("postCommentForCreatorPost", function(userName, userProfilePicUrl, postUUID, comment) {
        functions.postCommentForCreatorPost(userName, userProfilePicUrl, postUUID, comment, socket, amqpConn, config.rabbitmq.topics.postCommentForCreatorPost);
    });

    /**
     * on.deleteCreatorPost listens for post delete
     */
    socket.on("deleteCreatorPost", function(postUUID) {
        functions.deleteCreatorPost(postUUID, socket, amqpConn, config.rabbitmq.topics.deleteCreatorPost);
    });

    /**
     * on.connectWithCreator listens for new followers
     */
    socket.on("connectWithCreator", function(userId, userName, photoUrl, creatorName) {
        functions.connectWithCreator(userId, userName, photoUrl, creatorName, socket, amqpConn, config.rabbitmq.topics.connectWithCreator);
    });

    /**
     * on.disconnectWithCreator listens for new unfollowers
     */
    socket.on("disconnectWithCreator", function(userId, userName, photoUrl, creatorName) {
        functions.disconnectWithCreator(userId, userName, photoUrl, creatorName, socket, amqpConn, config.rabbitmq.topics.disconnectWithCreator);
    });

    /**
     * on.disconnect listens for disconnect events
     */
    socket.on("disconnect", function() {});
});


/**
 *  Uploads video post
 */
app.post("/uploadVideoPost", function(req, res) {
    functions.uploadVideoPost(req, res, amqpConn, config.rabbitmq.topics.uploadVideoPost);
});