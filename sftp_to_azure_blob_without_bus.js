const sftpPool = require('./sftpPool');
const _ = require("lodash");
const schedule = require("node-schedule");
const azure_storage = require("azure-storage");
const db = require("./dbPool");
const config = require("./config");
const moment = require("moment");
const fs = require("fs");
const Sequelize = require("sequelize");
const Op = Sequelize.Op;
const uuidv4 = require('uuid/v4');
const async = require('async');

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
        schedule.scheduleJob("*/1 * * * *", async (date) => {
            console.log(`${date} - Scheduler Invoked`);
            await this.getSFTPFilesListAndProcess();
        });
    }
    async getFileWithDiffFromDB(datas) {
        const sftp_files = _.map(datas, x => ({
            name: x.name,
            modifyTime: x.modifyTime.toString(),
            size: x.size.toString(),
            status: 'failed',
        }));
        let files = await db.files.findAll({
            raw: true,
            where: {
                [Op.or]: sftp_files
            }
        });
        console.log('getFileWithDiffFromDB - resultset', files);
        return _.isEmpty(files);
    }
    async getSFTPFilesListAndProcess() {
        const sftp = await sftpPool.acquire();
        const datas = await sftp.list(this.sftp_from_folder);
        async.eachLimit(datas, 5, (data, complete) => this.sendMessageToDB(data, complete), (err, result) => { // 4
            if(err)
             console.log("Failed to Get SFTP Files List And Process -", err);
        });
        sftpPool.release(sftp);
    }
    async sendMessageToDB(data, complete) {
        console.log(data);
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
            const body = {
                id: result.dataValues.id,
                name: data.name
            };
            try {
                await this.fetchSFTPFiletoLocalThenPushToAzureBlob(body);
                complete();
            } catch (e) {
                console.log(
                    "Failed to Fetch SFTP File to Local Then Push To Azure Blob :", e);
                await db.files.update({
                    status: "failed",
                    updated_at: moment(new Date()).format("YYYY-MM-DD HH:mm:ss")
                }, {
                    where: {
                        id: body.id
                    }
                });
                complete();
            }
        } else {
            complete();
        }
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
                    sftpPool.release(sftp);
                } else {
                    console.log("error createBlockBlobFromLocalFile", error);
                }
            }
        );
    }
}

sftp_to_azure_instance = new sftp_to_azure();
sftp_to_azure_instance.start();