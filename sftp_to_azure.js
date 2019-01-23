const sftpPool = require('./sftpPool');
const _ = require("lodash");
const schedule = require("node-schedule");
const azure = require("azure");
const azure_storage = require("azure-storage");
const db = require("./dbPool");
const config = require("./config");
const moment = require("moment");
const fs = require("fs");
const Sequelize = require("sequelize");
const Op = Sequelize.Op;
const queueSvc = azure.createQueueService();
const uuidv4 = require('uuid/v4');

class sftp_to_azure {
  constructor() {
    this.service_bus_topic_name = "sftp_topic";
    this.service_bus_queue_name = "sftpqueue";
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
      console.log(`${new Date()} - Scheduler - Sender Invoked`);
      await this.getSFTPFilesListAndSendToServiceBus();
  }

  async fetchSFTPFiletoLocalThenPushToAzureBlob(body) {
    const fileName = body.name;
    //console.log(body);
    const sftp = await sftpPool.acquire();
    let sftp_result = await sftp.fastGet(
      this.sftp_from_folder + "/" + fileName,
      "./tmp/" + fileName
    );
    console.log(sftp_result);
    let uniqueFilename = uuidv4() + '.' + fileName.split('.').pop();
    let blobService = azure_storage.createBlobService();
    blobService.createBlockBlobFromLocalFile(
      this.azure_storage_container_name,
      uniqueFilename,
      "./tmp/" + fileName,
      async (error, result) => {
        if (!error) {
          console.log(result);
          const deleteTmpFile = path =>
            new Promise((res, rej) => {
              fs.unlink(path, (err, data) => {
                if (err) rej(err);
                else res(data);
              });
            });
          await deleteTmpFile("./tmp/" + fileName);
          await db.files.update({
            status: "done",
            url: this.azure_storage_container_url + uniqueFilename,
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
          console.log("error createBlockBlobFromLocalFile", error);
        }
      }
    );
    sftpPool.release(sftp);
  }
  async getFileWithDiffFromDB(datas) {
    const sftp_files = _.map(datas, x => ({
      name: x.name,
      modifyTime: x.modifyTime.toString(),
      size: x.size.toString()
    }));
    let files = await db.files.findAll({
      raw: true,
      where: {
        [Op.or]: sftp_files
      }
    });
    // console.log('getFileWithDiffFromDB - resultset', files);
    return _.isEmpty(files);
  }
  async getSFTPFilesListAndSendToServiceBus() {
    const sftp = await sftpPool.acquire();
    const datas = await sftp.list(this.sftp_from_folder);
    _.forEach(datas, data => this.sendMessageToQueue(data));
    sftpPool.release(sftp);
  }
  createQueue() {
    queueSvc.createQueueIfNotExists(this.service_bus_queue_name, (error, results, response) => {
      if (!error) {
        // Queue created or exists
      }
    });
  }
  async sendMessageToQueue(data) {
    if (await this.getFileWithDiffFromDB([data])) {
      const payload = {
        name: data.name,
        modifyTime: data.modifyTime,
        size: data.size,
        created_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss"),
        status: "progress",
        updated_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss"),
        url: ""
      };
      console.log("the data info -", payload);
      const result = await db.files.create(payload);
      let message = JSON.stringify({
          id: result.dataValues.id,
          name: data.name
        });

      queueSvc.createMessage(this.service_bus_queue_name, message, (error, results, response) => {
          if (error) {
            console.log(error);
          } else {
            console.log("Message Sended to Servcie Bus -", message);
          }
      });

    }
  }

  recieveMessageFromQueue() {
    queueSvc.getMessages(this.service_bus_queue_name, {
      numOfMessages: 10,
      visibilityTimeout: 1 * 60
    }, async (error, results, getResponse) => {
      if (!error) {
        // Messages retrieved
        for (let index in results) {
          // text is available in result[index].messageText
          try {
            let message = results[index];
            const messageText = JSON.parse(message.messageText);
            console.log("Recieved Message from Servcie Bus - ", messageText);
            await this.fetchSFTPFiletoLocalThenPushToAzureBlob(messageText);
            queueSvc.deleteMessage(this.service_bus_queue_name, message.messageId, message.popReceipt, (error, deleteResponse) => {
              if (!error) {
                console.log("Deleted Message from Servcie Bus  - ", messageText);
              }
            });
          } catch (e) {
            console.log(
              "Exception Occured in Func recieveMessageFromQueue - ", e);
          }
        }
      }
    });
  }
}
module.exports = sftp_to_azure;