let Client = require('ssh2-sftp-client');
let sftp = new Client();
let _ = require('lodash');
var schedule = require('node-schedule');


schedule.scheduleJob('28 * * * *', function () {
    console.log('The answer to life, the universe, and everything!');
    createSftp();
});

function uploadToBlob(name, stream) {
    var azure = require('azure-storage');
    var blobService = azure.createBlobService();
    stream.pipe(blobService.createWriteStreamToBlockBlob('prabafiles', name));
}

function sftpReadStream(data) {
    sftp.get('upload' + '/' + data.name)
        .then((stream) => {
            //const chunks = [];
            //stream.on('data', (chunk) => {
            uploadToBlob(data.name, stream);
            //});
            stream.on('end', () => {
                console.log('done -', data.name);
            });
        }).catch((err) => {
            console.log('catch err:', err);
        });
}

function createSftp() {
    sftp.connect({
        host: '52.187.134.188',
        port: '8080',
        username: 'foo',
        password: 'pass'
    }).then(() => {
        return sftp.list('upload');
    }).then((datas) => {
        // console.log(datas, 'the data info');
        _.forEach(datas, data => sftpReadStream(data))
    }).catch((err) => {
        console.log(err, 'catch error');
    });
}

var azure = require('azure');
var serviceBusService = azure.createServiceBusService();

function createTopic() {
    var topicOptions = {
        MaxSizeInMegabytes: '5120',
        DefaultMessageTimeToLive: 'PT1M'
    };

    serviceBusService.createTopicIfNotExists('sftp_topic', topicOptions, function (error) {
        if (!error) {
            // topic was created or exists
        }
    });
}
function createSubscription() {
    serviceBusService.createSubscription('sftp_topic', 'HighMessages', function (error){
        if(!error){
            // subscription created
            rule.create();
        }
    });
    var rule={
        deleteDefault: function(){
            serviceBusService.deleteRule('sftp_topic',
                'HighMessages',
                azure.Constants.ServiceBusConstants.DEFAULT_RULE_NAME,
                rule.handleError);
        },
        create: function(){
            var ruleOptions = {
                sqlExpressionFilter: 'messagenumber > 3'
            };
            rule.deleteDefault();
            serviceBusService.createRule('sftp_topic',
                'HighMessages',
                'HighMessageFilter',
                ruleOptions,
                rule.handleError);
        },
        handleError: function(error){
            if(error){
                console.log(error)
            }
        }
    }
}
function sendMessage() {
    var message = {
        body: '',
        customProperties: {
            messagenumber: 0
        }
    }
    
    for (i = 0;i < 5;i++) {
        message.customProperties.messagenumber=i;
        message.body='This is Message #'+i;
        serviceBusService.sendTopicMessage(topic, message, function(error) {
          if (error) {
            console.log(error);
          }
        });
    }
}
function recieveMessage() {
    serviceBusService.receiveSubscriptionMessage('sftp_topic', 'HighMessages', { isPeekLock: true }, function(error, lockedMessage){
        if(!error){
            // Message received and locked
            console.log(lockedMessage);
            serviceBusService.deleteMessage(lockedMessage, function (deleteError){
                if(!deleteError){
                    // Message deleted
                    console.log('message has been deleted.');
                }
            })
        }
    });
}
createTopic();
createSubscription();
recieveMessage();
sendMessage();