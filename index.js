let Client = require("ssh2-sftp-client");
let sftp = new Client();
let _ = require("lodash");
let schedule = require("node-schedule");
let azure = require("azure");
let serviceBusService = azure.createServiceBusService();
var azure_storage = require("azure-storage");
let db = require("./db");
let config = require("./config");
var promisePipe = require("promisepipe");
let moment = require("moment");

class sftp_to_azure {
  constructor() {
    this.service_bus_topic_name = "sftp_topic";
    this.sftp_config = {
      host: config.SFTP_HOST,
      port: config.SFTP_PORT,
      username: config.SFTP_USERNAME,
      password: config.SFTP_PASSWORD
    };
    this.sftp_from_folder = "upload/from";
    this.sftp_to_folder = "upload/to";
    this.blob_url =
      "https://prabastorage.blob.core.windows.net/prabafiles/upload/";
  }
  async start() {
    await sftp.connect(this.sftp_config);
    sftp.on("end", () => {
      console.log("sftp end event");
    });

    sftp.on("close", () => {
      console.log("sftp close event");
    });
    this.getSftpFilesList();
    schedule.scheduleJob("*/1 * * * *", () => {
      console.log("The answer to life, the universe, and everything!");
       //this.getSftpFilesList();
    });
    // this.fetchSFTPfile({name: "333.txt"})
  }

  async uploadToBlob(name, stream) {
    var blobService = azure_storage.createBlobService();
    await promisePipe(
      stream,
      blobService.createWriteStreamToBlockBlob("prabafiles", name)
    );
    //stream.pipe(blobService.createWriteStreamToBlockBlob('prabafiles', body.name));
  }

  async fetchSFTPfile(body) {
    // console.log(body);
    const fileName = body.name;
    let stream = await sftp.get(this.sftp_from_folder + "/" + fileName);
    await this.uploadToBlob(fileName, stream);
    let obj = await db.files.findOne({
      where: {
        id: body.id
      }
    });
    await obj.update({
      status: "done",
      url: this.blob_url + fileName,
      updated_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss")
    });
    if (await sftp.exists(this.sftp_to_folder + "/" + body.name)) {
      await sftp.delete(this.sftp_to_folder + "/" + body.name);
    }
    await sftp.rename(
      this.sftp_from_folder + "/" + fileName,
      this.sftp_to_folder + "/" + fileName
    );
  }

  getSftpFilesList() {
    sftp
      .list(this.sftp_from_folder)
      .then(datas => {
        //console.log(datas, 'the data info');
        _.forEach(datas, data => this.sendMessage(data.name));
      })
      .catch(err => {
        console.log(err, "catch error");
      });
  }

  createTopic() {
    var topicOptions = {
      MaxSizeInMegabytes: "5120",
      DefaultMessageTimeToLive: "PT1M"
    };

    serviceBusService.createTopicIfNotExists(
      "sftp_topic",
      topicOptions,
      function(error) {
        if (!error) {
          console.log("topic created");
        }
      }
    );
  }

  createSubscription() {
    serviceBusService.createSubscription(
      this.service_bus_topic_name,
      "HighMessages",
      function(error) {
        if (!error) {
          // subscription created
          rule.create();
        }
      }
    );
    var rule = {
      deleteDefault: function() {
        serviceBusService.deleteRule(
          this.service_bus_topic_name,
          "HighMessages",
          azure.Constants.ServiceBusConstants.DEFAULT_RULE_NAME,
          rule.handleError
        );
      },
      create: function() {
        var ruleOptions = {
          sqlExpressionFilter: "messagenumber > 3"
        };
        rule.deleteDefault();
        serviceBusService.createRule(
          this.service_bus_topic_name,
          "HighMessages",
          "HighMessageFilter",
          ruleOptions,
          rule.handleError
        );
      },
      handleError: function(error) {
        if (error) {
          console.log(error);
        }
      }
    };
  }

  async sendMessage(message) {
    const result = await db.files.create({
      name: message,
      created_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss"),
      status: "progress",
      updated_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss"),
      url: ""
    });
    var message = {
      body: JSON.stringify({
        id: result.dataValues.id,
        name: message
      }),
      customProperties: {
        messagenumber: 5
      }
    };
    serviceBusService.sendTopicMessage(
      this.service_bus_topic_name,
      message,
      function(error) {
        if (error) {
          console.log(error);
        } else {
        }
      }
    );
  }

  recieveMessage() {
    serviceBusService.receiveSubscriptionMessage(
      this.service_bus_topic_name,
      "HighMessages",
      {
        isPeekLock: true
      },
      (error, lockedMessage) => {
        if (!error) {
          // Message received and locked
          // console.log(lockedMessage);
          try {
            this.fetchSFTPfile(JSON.parse(lockedMessage.body));
            console.log("recieveMessage", JSON.parse(lockedMessage.body));
            // serviceBusService.deleteMessage(lockedMessage, deleteError => {
            //   if (!deleteError) {
            //     // Message deleted
            //     console.log("Deleted ", JSON.parse(lockedMessage.body));
            //   }
            // });
          } catch (e) {
            console.log(e);
          }
        }
      }
    );
  }
}
const sftp_to_azure_instance = new sftp_to_azure();

// sftp_to_azure_instance.createTopic();
// sftp_to_azure_instance.createSubscription();
setInterval(function() {
  sftp_to_azure_instance.recieveMessage();
}, 1000);

sftp_to_azure_instance.start();
