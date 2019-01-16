let Client = require("ssh2-sftp-client");
let sftp = new Client();
let _ = require("lodash");
let schedule = require("node-schedule");
let azure = require("azure");
let serviceBusService = azure.createServiceBusService();
var azure_storage = require("azure-storage");
let db = require("./db");
let config = require("./config");
let moment = require("moment");
const fs = require('fs')
const Sequelize = require("sequelize");
const Op = Sequelize.Op;

class sftp_to_azure {
  constructor() {
    this.service_bus_topic_name = "sftp_topic";
    this.service_bus_queue_name = "sftp_queue";
    this.sftp_config = {
      host: config.SFTP_HOST,
      port: config.SFTP_PORT,
      username: config.SFTP_USERNAME,
      password: config.SFTP_PASSWORD
    };
    this.sftp_from_folder = "upload/from";
    this.sftp_to_folder = "upload/to";
    this.azure_storage_container_url =
      "https://prabastorage.blob.core.windows.net/prabafiles/";
    this.azure_storage_container_name = "prabafiles";
  }
  async start() {
    await sftp.connect(this.sftp_config);
    sftp.on("end", () => {
      console.log("sftp end event");
    });
    sftp.on("close", () => {
      console.log("sftp close event");
    });
    //this.getSFTPFilesListAndSendToServiceBus();
    schedule.scheduleJob("*/1 * * * *", () => {
      console.log("The answer to life, the universe, and everything!");
      this.getSFTPFilesListAndSendToServiceBus();
    });
  }

  async uploadToBlob(name, stream) {
    var blobService = azure_storage.createBlobService();
    stream.pipe(blobService.createWriteStreamToBlockBlob(this.azure_storage_container_name, name));
  }
  async fetchSFTPFiletoLocalThenPushToAzureBlob(body) {
    const fileName = body.name;
    //console.log(body);
    let sftp_result = await sftp.fastGet(this.sftp_from_folder + "/" + fileName, "./tmp/" + fileName);
    console.log(sftp_result);
    var blobService = azure_storage.createBlobService();
    blobService.createBlockBlobFromLocalFile(this.azure_storage_container_name, fileName, "./tmp/" + fileName, async (error, result) => {
      if (!error) {
        console.log(result);
        const deleteTmpFile = (path) =>
          new Promise((res, rej) => {
            fs.unlink(path, (err, data) => {
              if (err) rej(err)
              else res(data)
            })
          })
        await deleteTmpFile("./tmp/" + fileName);
        await db.files.update({
          status: "done",
          url: this.azure_storage_container_url + fileName,
          updated_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss")
        }, {
          where: {
            id: body.id
          }
        });
        if (await sftp.exists(this.sftp_to_folder + "/" + fileName)) {
          await sftp.delete(this.sftp_to_folder + "/" + fileName);
        }
        await sftp.rename(
          this.sftp_from_folder + "/" + fileName,
          this.sftp_to_folder + "/" + fileName
        );
      } else {
        console.log('error createBlockBlobFromLocalFile', error);
      }
    });
  }
  async getFilesWithDiffFromDB(datas) {
    const sftp_files = _.map(datas, (x) => ({
      name: x.name,
      modifyTime: x.modifyTime.toString(),
      size: x.size.toString()
    }));
    // console.log(sftp_files);
    let files = await db.files.findAll({
      raw: true,
      where: {
        [Op.or]: sftp_files,
      }
    });
    files = _.map(files, file => _.pick(file, ['name', 'size', 'modifyTime']));
    const diff =  _.differenceWith(sftp_files, files, _.isEqual);
    return diff;
  }
  getSFTPFilesListAndSendToServiceBus() {
    sftp
      .list(this.sftp_from_folder)
      .then(async (datas) => {
        const files = await this.getFilesWithDiffFromDB(datas)
        console.log(files);
        _.forEach(files, data => this.sendMessageToQueue(data));
      })
      .catch(err => {
        console.log("catch error -", err);
      });
  }
  createQueue() {
    var queueOptions = {
      MaxSizeInMegabytes: '5120',
      DefaultMessageTimeToLive: 'PT2M'
    };
    serviceBusService.createQueueIfNotExists(this.service_bus_queue_name, queueOptions, (error) => {
      if (!error) {
        // Queue exists
      }
    });
  }
  async sendMessageToQueue(data) {
    const payload = {
      name: data.name,
      modifyTime: data.modifyTime,
      size: data.size,
      created_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss"),
      status: "progress",
      updated_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss"),
      url: ""
    };
    console.log('the data info -', payload);
    const result = await db.files.create(payload);
    var message = {
      body: JSON.stringify({
        id: result.dataValues.id,
        name: data.name
      })
    };
    serviceBusService.sendQueueMessage(
      this.service_bus_queue_name,
      message,
      (error) => {
        if (error) {
          console.log(error);
        } else {
          console.log("Message Sended to Servcie Bus -", message);
        }
      }
    );
  }

  recieveMessageFromQueue() {
    serviceBusService.receiveQueueMessage(this.service_bus_queue_name, {
        isPeekLock: true
      },
      async (error, lockedMessage) => {
        if (!error) {
          // Message received and locked
          // console.log(lockedMessage);
          try {
            console.log("Recieved Message from Servcie Bus - ", JSON.parse(lockedMessage.body));
            await this.fetchSFTPFiletoLocalThenPushToAzureBlob(JSON.parse(lockedMessage.body));
            serviceBusService.deleteMessage(lockedMessage, deleteError => {
              if (!deleteError) {
                console.log("Deleted Message from Servcie Bus  - ", JSON.parse(lockedMessage.body));
              }
            });
          } catch (e) {
            console.log("Exception Occured in Func recieveMessageFromQueue - ", e);
          }
        }
      }
    );
  }
}
const sftp_to_azure_instance = new sftp_to_azure();

//sftp_to_azure_instance.createQueue();
//sftp_to_azure_instance.createTopicSubscription();
setInterval(() => {
  sftp_to_azure_instance.recieveMessageFromQueue();
}, 1000);

sftp_to_azure_instance.start();