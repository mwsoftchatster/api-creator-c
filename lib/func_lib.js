/* jshint esnext: true */
var fs = require('fs');
var path = require('path');
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-creator-c/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-creator-c/lib/email_lib.js');
var time = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-creator-c/lib/time_lib.js');
var aws = require("aws-sdk");
var s3 = new aws.S3();
var admin = require('firebase-admin');
var serviceAccount = require(config.firebase.service_account);
admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: config.firebase.databaseURL
});
var db = admin.database();
var ref = db.ref();
var creatorPostsRef = ref.child('creator_posts');
var creatorPostLikesRef = ref.child('creator_post_likes');
var creatorPostUnLikesRef = ref.child('creator_post_unlikes');
var creatorFollowsRef = ref.child('creator_follows');
var creatorPostCommentsRef = ref.child('creator_post_comments');
var creatorUnFollowsRef = ref.child('creator_unfollows');
var rn = require('random-number');
var gen = rn.generator({
    min: 1000,
    max: 9999,
    integer: true
});
var contentType = require('content-type');
var fileType = require('file-type');
var multer = require('multer');
const uploadVideoPost = multer({
    dest: 'videos/',
    limits: { fileSize: 10000000, files: 1 },
    fileFilter: (req, file, callback) => {
        if (!file.originalname.match(/\.(mp4)$/)) {
            return callback(new Error('Only MP4 Videos are allowed !'), false)
        }
        callback(null, true);
    }
}).single('video');


/**
 *  Publishes message on ApiCreatorQ topic
 */
function publishToApiCreatorQ(message, amqpConn, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiCreatorQ.*';
            var key = 'apiCreatorQ.' + topic;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, key, new Buffer(message));
        });
    }
}

/**
 *  Publishes message on ApiCreatorQ topic
 */
function publishToApiUserC(message, amqpConn, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiUserC.*';
            var key = 'apiUserC.' + topic;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, key, new Buffer(message));
        });
    }
}

/**
 *  Setup the pool of connections to the db so that every connection can be reused upon it's release
 */
var mysql = require('mysql');
var Sequelize = require('sequelize');
const sequelize = new Sequelize(config.db.name, config.db.user_name, config.db.password, {
    host: config.db.host,
    dialect: config.db.dialect,
    port: config.db.port,
    operatorsAliases: config.db.operatorsAliases,
    pool: {
      max: config.db.pool.max,
      min: config.db.pool.min,
      acquire: config.db.pool.acquire,
      idle: config.db.pool.idle
    }
});


/**
 *  Saves new creator into database
 *
 * (creator Object): Object that holds all the creator data
 * (amqpConn Object): RabbitMQ connection object that is used to send message on topic
 * (topic String): Topic on which message is to be published
 */
module.exports.saveNewCreator = function (creator, amqpConn, topic) {
    sequelize.query('CALL SaveNewCreator(?,?,?,?,?,?,?,?)',
    { replacements: [ creator.creatorId, creator.creatorName, creator.profilePic, creator.statusMessage, creator.creatorPosts, creator.creatorProfileViews, 
        creator.creatorTotalLikes, creator.creatorWebsite ], type: sequelize.QueryTypes.RAW }).then(result => {
            publishToApiCreatorQ(JSON.stringify(creator), amqpConn, topic);

            var response = {
                status: config.rabbitmq.statuses.ok
            };
            publishToApiUserC(JSON.stringify(response), amqpConn, config.rabbitmq.topics.newCreatorC);
    }).error(function(err){
        email.sendApiCreatorCErrorEmail(err);
    });
}


/**
 *  Stores creators text post
 *
 * (userName String): creators user name
 * (postCapture String): text that appears under the post
 * (creatorProfilePic String): String that holds the URL of profile image of creator
 * (post String): post text
 * (postUUID String): uuid of the post
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.saveCreatorTextPost = function (userName, postCapture, postType, creatorProfilePic, postText, postUUID, socket, amqpConn, topic) {
    var myutc = time.getCurrentUTC();
    var myutcForPostName = myutc.replace(' ', '_');
    myutcForPostName = myutcForPostName.replace('-', '_');
    myutcForPostName = myutcForPostName.replace('-', '_');
    myutcForPostName = myutcForPostName.replace(':', '_');
    myutcForPostName = myutcForPostName.replace(':', '_');

    sequelize.query('CALL ProcessNewCreatorTextPost(?,?,?,?,?,?,?,?,?,?,?)',
    { replacements: [ postUUID, userName, postCapture, postType, postText, 0, 0, myutc, myutc, "post", userName+" uploaded new post." ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            var message = {
                postUUID: postUUID,
                userName: userName,
                postCapture: postCapture,
                postType: postType,
                postText: postText,
                likes: 0,
                comments: 0,
                createdAt: myutc,
                lastUpdatedAt: myutc,
                type: "post",
                notificationMessage: userName+" uploaded new post."
            };
            publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

            if(result.length > 0){
                var receiver_ids = [];
                for(var i = 0; i < result.length; i++){
                    receiver_ids.push(result[i].follower_id);
                }
                // save post reference to firebase to trigger cloud function
                var firebaseCreatorPost = {
                    receiver_ids: receiver_ids
                };
                creatorPostsRef.child(postUUID).set(firebaseCreatorPost);
            }
            socket.emit("saveCreatorTextPost","success");
    }).error(function(err){
        email.sendApiCreatorCErrorEmail(err);
        socket.emit("saveCreatorTextPost","error");
    });
};


/**
 *  Stores creators image post
 *
 * (userName String): creators user name
 * (postCapture String): text that appears under the post
 * (creatorProfilePic String): String that holds the URL of profile image of creator
 * (post String): base64 encoded String that holds the image
 * (postUUID String): uuid of the post
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.saveCreatorPost = function (userName, postCapture, postType, creatorProfilePic, post, postUUID, socket, amqpConn, topic) {
  var myutc = time.getCurrentUTC();
  var myutcForPostName = myutc.replace(' ', '_');
  myutcForPostName = myutcForPostName.replace('-', '_');
  myutcForPostName = myutcForPostName.replace('-', '_');
  myutcForPostName = myutcForPostName.replace(':', '_');
  myutcForPostName = myutcForPostName.replace(':', '_');
  var postName = myutcForPostName.concat('_').concat(userName);
  let params = {
      Bucket: 'chatster-creator-posts',
      Key: `${userName}/${postName}.jpg`,
      Body: new Buffer(post, 'base64'),
      ContentEncoding: 'base64',
      ContentType: 'image/jpg'
  };

  var postUrl = "";
  //s3.putObject(params, function(err, data) {
  //   if (!err) {
          postUrl = `//d1rtocr1p2vc61.cloudfront.net/${params.Key}`;
          sequelize.query('CALL ProcessNewCreatorPost(?,?,?,?,?,?,?,?,?,?,?)',
          { replacements: [ postUUID, userName, postCapture, postType, 0, 0, myutc, myutc, "post", userName+" uploaded new post.", postUrl ],
              type: sequelize.QueryTypes.RAW }).then(result => {
                var message = {
                    postUUID: postUUID,
                    userName: userName,
                    postCapture: postCapture,
                    postType: postType,
                    likes: 0,
                    comments: 0,
                    createdAt: myutc,
                    lastUpdatedAt: myutc,
                    type: "post",
                    notificationMessage: userName+" uploaded new post.",
                    postUrl: postUrl
                };
                publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

                  if(result.length > 0){
                      var receiver_ids = [];
                      for(var i = 0; i < result.length; i++){
                          receiver_ids.push(result[i].follower_id);
                      }
                      // save post reference to firebase to trigger cloud function
                      var firebaseCreatorPost = {
                          receiver_ids: receiver_ids
                      };
                      creatorPostsRef.child(postUUID).set(firebaseCreatorPost);
                  }
                  socket.emit("saveCreatorPost","success");
          }).error(function(err){
              email.sendApiCreatorCErrorEmail(err);
              socket.emit("saveCreatorPost","error");
          });
    //   } else {
    //       email.sendApiCreatorCErrorEmail(err);
    //       // emit error event to the creator
    //       socket.emit("saveCreatorPost","error");
    //   }
  //});
};

/**
 *  Updates creators post, not implemented yet
 *
 * (userName String): creators user name
 * (postCapture String): text that appears under the post
 * (creatorProfilePic String): String that holds the URL of profile image of creator
 * (post String): base64 encoded String that holds the image
 * (postUUID String): uuid of the post
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.updateCreatorPost = function (userName, postCapture, creatorProfilePic, post, postUUID, socket, amqpConn, topic) {
  var myutc = time.getCurrentUTC();
  var postName = myutc.concat('_').concat(userId);
  let params = {
      Bucket: 'chatster-creator-posts',
      Key: `${userName}/${postName}.jpg`,
      Body: new Buffer(post, 'base64'),
      ContentEncoding: 'base64',
      ContentType: 'image/jpg'
  };
  s3.putObject(params, function(err, data) {
      if (!err) {
          // `//s3-us-west-2.amazonaws.com/chatster-creator-posts/${params.Key}`
          // update creators post in db
          
          // publish to api-creator-q
          var message = {
            userName: userName,
            postCapture: postCapture,
            creatorProfilePic: creatorProfilePic,
            post: post,
            postUUID: postUUID,
            lastUpdatedAt: myutc,
            type: "post"
        };
        publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);
          // emit success event to the creator
      } else {
          email.sendApiCreatorCErrorEmail(err);
          // emit error event to the creator
      }
  });
};


/**
 *  Likes creators post
 *
 * (userName String): creators user name
 * (postUUID String): uuid of the post
 * (userProfilePicUrl String): String that holds the URL of profile image of the user who likes this post
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.likeCreatorPost = function (userName, postUUID, userProfilePicUrl, socket, amqpConn, topic) {
  var myutc = time.getCurrentUTC();
  var updatePostLikesStatusErrorMsg = {
      uuid: postUUID,
      status: "error",
      updatedLikes: 0//this needs to be not sent
  };
  var updatePostLikesStatusSuccessMsg = {
      uuid: postUUID,
      status: "success",
      updatedLikes: 0//this needs to be not sent
  };

  sequelize.query('CALL ProcessNewCreatorPostLike(?,?,?,?,?)',
  { replacements: [ postUUID, userName, myutc, "postLike", userName+" liked your post." ],
      type: sequelize.QueryTypes.RAW }).then(result => {
        var message = {
            postUUID: postUUID,
            userName: userName,
            lastUpdatedAt: myutc,
            type: "postLike",
            notificationMessage: userName+" liked your post."
        };
        publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

        if(result.length > 0){
            // save post like reference to firebase to trigger cloud function
            var firebaseCreatorPostLike = {
                creator_id: result[0].creator_id
            };
            var code = gen();
            var postLikeRef = postUUID+result[0].creator_name+code;
            creatorPostLikesRef.child(postLikeRef).set(firebaseCreatorPostLike);
            socket.emit("likeCreatorPost", updatePostLikesStatusSuccessMsg);
        }
  }).error(function(err){
      email.sendApiCreatorCErrorEmail(err);
      socket.emit("likeCreatorPost", updatePostLikesStatusErrorMsg);
  });
};


/**
 *  Unlikes creators post
 *
 * (userName String): creators user name
 * (postUUID String): uuid of the post
 * (userProfilePicUrl String): String that holds the URL of profile image of the user who unlikes this post
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.unlikeCreatorPost = function (userName, postUUID, userProfilePicUrl, socket, amqpConn, topic) {
  var myutc = time.getCurrentUTC();
  var updatePostUnLikesStatusErrorMsg = {
      uuid: postUUID,
      status: "error",
      updatedLikes: 0//this needs to be not sent
  };
  var updatePostUnLikesStatusSuccessMsg = {
      uuid: postUUID,
      status: "success",
      updatedLikes: 0//this needs to be not sent
  };

  sequelize.query('CALL ProcessNewCreatorPostDisLike(?,?,?,?,?)',
  { replacements: [ postUUID, userName, myutc, "postUnlike", userName+" unliked your post." ],
      type: sequelize.QueryTypes.RAW }).then(result => {
        var message = {
            postUUID: postUUID,
            userName: userName,
            lastUpdatedAt: myutc,
            type: "postUnlike",
            notificationMessage: userName+" unliked your post."
        };
        publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

          if(result.length > 0){
              var firebaseCreatorPostUnLike = {
                  creator_id: result[0].creator_id
              };
              var code = gen();
              var postUnLikeRef = postUUID+result[0].creator_name+code;
              creatorPostUnLikesRef.child(postUnLikeRef).set(firebaseCreatorPostUnLike);
              socket.emit("unlikeCreatorPost", updatePostUnLikesStatusSuccessMsg);
          }
  }).error(function(err){
      email.sendApiCreatorCErrorEmail(err);
      socket.emit("unlikeCreatorPost", updatePostUnLikesStatusErrorMsg);
  });
};


/**
 *  Saves comment on creators post
 *
 * (userName String): creators user name
 * (userProfilePicUrl String): String that holds the URL of profile image of the user who comments on this post
 * (postUUID String): uuid of the post
 * (comment String): comment for the post
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.postCommentForCreatorPost = function (userName, userProfilePicUrl, postUUID, comment, socket, amqpConn, topic) {
  var myutc = time.getCurrentUTC();
  sequelize.query('CALL ProcessNewCreatorPostComment(?,?,?,?,?,?,?)',
  { replacements: [ postUUID, userName, comment, myutc, myutc,"postComment", userName + " wrote this comment for your post: " + comment ],
      type: sequelize.QueryTypes.RAW }).then(result => {
        var message = {
            postUUID: postUUID,
            userName: userName,
            comment: comment,
            createdAt: myutc,
            lastUpdatedAt: myutc,
            type: "postComment",
            notificationMessage: userName + " wrote this comment for your post: " + comment
        };
        publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

        if(result.length > 0){
            var firebaseCreatorPostComment = {
                creator_id: result[0].creator_id
            };
            var code = gen();
            var creatorPostCommentRef = postUUID+userName+code;
            creatorPostCommentsRef.child(creatorPostCommentRef).set(firebaseCreatorPostComment);
            socket.emit("postCommentForCreatorPost","success");
        }
  }).error(function(err){
      email.sendApiCreatorCErrorEmail(err);
      socket.emit("postCommentForCreatorPost","error");
  });
};


/**
 *  Deletes creators post
 *
 * (postUUID String): uuid of the post
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.deleteCreatorPost = function (postUUID, socket, amqpConn, topic) {
  // delete post with id from db
  // notify user of success failure
  var deleteResponseError = {
      uuid: postUUID,
      status: "error"
  };
  var deleteResponseSuccess = {
      uuid: postUUID,
      status: "success"
  };

  sequelize.query('CALL DeleteCreatorPost(?)',
  { replacements: [ postUUID ],
      type: sequelize.QueryTypes.RAW }).then(result => {
        var message = {
            postUUID: postUUID
        };
        publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

        socket.emit("deleteCreatorPost",deleteResponseSuccess);
  }).error(function(err){
      email.sendApiCreatorCErrorEmail(err);
      socket.emit("deleteCreatorPost",deleteResponseError);
  });
};


/**
 *  Connects this creator another with creator
 *
 * (userId String): userId of creator who wants to connect with another creator
 * (userName String): user name of creator who wants to connect with another creator
 * (photoUrl String): user profile picture URL of creator who wants to connect with another creator
 * (creatorName String): creator name of creator with whom this user wants to connect
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.connectWithCreator = function (userId, userName, photoUrl, creatorName, socket, amqpConn, topic) {
  var myutc = time.getCurrentUTC();
  var followResponseError = {
      creatorsName: creatorName,
      status: "error"
  };
  var followResponseSuccess = {
      creatorsName: creatorName,
      status: "success"
  };

  sequelize.query('CALL ProcessNewCreatorFollower(?,?,?,?,?)',
  { replacements: [ userId.toString(), creatorName, "follow", userName + " is following you.", myutc ],
      type: sequelize.QueryTypes.RAW }).then(result => {
        var message = {
            userId: userId.toString(),
            creatorName: creatorName,
            type: "follow",
            notificationMessage: userName +  "is following you.",
            createdAt: myutc
        };
        publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

        if(result.length > 0){
            var firebaseCreatorFollow = {
                creator_id: result[0].creator_id
            };
            var code = gen();
            var creatorFollowRef = userId+result[0].creator_name+code;
            creatorFollowsRef.child(creatorFollowRef).set(firebaseCreatorFollow);
            socket.emit("connectWithCreator",followResponseSuccess);
        }
  }).error(function(err){
      email.sendApiCreatorCErrorEmail(err);
      socket.emit("connectWithCreator",followResponseError);
  });
};


/**
 *  Disconnects from this creator
 *
 * (userId String): userId of creator who wants to disconnect with another creator
 * (userName String): user name of creator who wants to disconnect with another creator
 * (photoUrl String): user profile picture URL of creator who wants to disconnect with another creator
 * (creatorName String): creator name of creator with whom this user wants to disconnect
 * (socket Object): Socket.IO object that is used to send user response
 */
module.exports.disconnectWithCreator = function (userId, userName, photoUrl, creatorName, socket, amqpConn, topic) {
  var myutc = time.getCurrentUTC();
  var unfollowResponseError = {
      creatorsName: creatorName,
      status: "error"
  };
  var unfollowResponseSuccess = {
      creatorsName: creatorName,
      status: "success"
  };
  
  sequelize.query('CALL ProcessDeleteCreatorFollower(?,?,?,?,?)',
  { replacements: [ userId.toString(), creatorName, "unfollow", userName + " has unfollowed you.", myutc ],
      type: sequelize.QueryTypes.RAW }).then(result => {
        var message = {
            userId: userId.toString(),
            creatorName: creatorName,
            type: "unfollow",
            notificationMessage: userName +  "has unfollowed you.",
            createdAt: myutc
        };
        publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

        if(result.length > 0){
            var firebaseCreatorUnFollow = {
                creator_id: result[0].creator_id
            };
            var code = gen();
            var creatorUnFollowRef = userId+creatorName+code;
            creatorUnFollowsRef.child(creatorUnFollowRef).set(firebaseCreatorUnFollow);
            socket.emit("disconnectWithCreator",unfollowResponseSuccess);
        }
  }).error(function(err){
      email.sendApiCreatorCErrorEmail(err);
      socket.emit("disconnectWithCreator",unfollowResponseError);
  });
};


/**
 *  Stores creators video post
 *
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
function saveCreatorVideoPost(req, res, amqpConn, topic) {
    // userName, postCapture, creatorProfilePic, post path, postUUID, postType
    fs.readFile(req.file.path, function (err, video) {
        if (err){
            email.sendApiCreatorCErrorEmail(err);
            res.json("error");
        }else{
            var myutc = time.getCurrentUTC();
            var myutcForPostName = myutc.replace(' ', '_');
            myutcForPostName = myutcForPostName.replace('-', '_');
            myutcForPostName = myutcForPostName.replace('-', '_');
            myutcForPostName = myutcForPostName.replace(':', '_');
            myutcForPostName = myutcForPostName.replace(':', '_');
            var postName = myutcForPostName.concat('_').concat(req.query.userName);
            let params = {
                Bucket: 'chatster-creator-posts',
                Key: `${req.query.userName}/${postName}.mp4`,
                Body: video,
                ContentType: 'video/mp4'
            };
            var postUrl = "";
            s3.putObject(params, function(err, data) {
                if (!err) {
                    postUrl = `//d1rtocr1p2vc61.cloudfront.net/${params.Key}`;
                    sequelize.query('CALL ProcessNewCreatorPost(?,?,?,?,?,?,?,?,?,?,?)',
                    { replacements: [ req.query.uuid, req.query.userName, req.query.postCapture, req.query.postType, 0, 0, myutc, myutc, "post", req.query.userName+" uploaded new post.", postUrl ],
                        type: sequelize.QueryTypes.RAW }).then(result => {
                            var message = {
                                postUUID: req.query.uuid,
                                userName: req.query.userName,
                                postCapture: req.query.postCapture,
                                postType: req.query.postType,
                                likes: 0,
                                comments: 0,
                                createdAt: myutc,
                                lastUpdatedAt: myutc,
                                type: "post",
                                notificationMessage: userName+" uploaded new post.",
                                postUrl: postUrl
                            };
                            publishToApiCreatorQ(JSON.stringify(message), amqpConn, topic);

                            if(result.length > 0){
                                var receiver_ids = [];
                                for(var i = 0; i < result.length; i++){
                                    receiver_ids.push(result[i].follower_id);
                                }
                                // save post reference to firebase to trigger cloud function
                                var firebaseCreatorPost = {
                                    receiver_ids: receiver_ids
                                };
                                creatorPostsRef.child(req.query.uuid).set(firebaseCreatorPost);
                                res.json("success");
                            }else{
                                res.json("success"); 
                            }
                    }).error(function(err){
                        email.sendApiCreatorCErrorEmail(err);
                        res.json("error");
                    });
                } else {
                    email.sendApiCreatorCErrorEmail(err);
                    res.json("error");
                }
            });
        }
    });
}


/**
 *  Uploads new video post to S3 and saves post data to db
 *
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.uploadVideoPost = function (req, res, amqpConn, topic){
    // userName, postCapture, creatorProfilePic, post, postUUID,
    uploadVideoPost(req, res, function(err) {
        if (err) {
            email.sendApiCreatorCErrorEmail(err);
            res.json("error");
        } else {
            saveCreatorVideoPost(req, res, amqpConn, topic);
        }
    })
};