const genericPool = require("generic-pool");
const Client = require("ssh2-sftp-client");
const config = require("./config");
const _ = require("lodash");

const sftp_config = {
    host: config.SFTP_HOST,
    port: config.SFTP_PORT,
    username: config.SFTP_USERNAME,
    password: config.SFTP_PASSWORD,
    keepaliveInterval: 1000,
};

const factory = {
    create: async () => {
        const sftp = new Client();
        sftp.on('ready', () => {
            console.log('SFTP - Connected')
        });
        sftp.on("end", async () => {
            console.log("SFTP - End Event");
        });
        sftp.on("close", async () => {
            console.log("SFTP - Close Event");
        });
        sftp.on("error", (err) => {
            console.log("SFTP - Connection Error - ", err);
            sftp.end();
        });
        await sftp.connect(sftp_config);
        return sftp;
    },
    destroy: (client) => {
        client.end();
    }
};

const opts = {
    max: 10, // maximum size of the pool
    min: 1 // minimum size of the pool
};

const myPool = genericPool.createPool(factory, opts);

module.exports = myPool;
//const moment = require("moment");
//const resourcePromise = myPool.acquire();
// resourcePromise
//     .then(async (sftp) => {
//         const sftp_to_folder = 'upload/to/' + moment(new Date()).format("YYYY-MM-DD");
//         if (!await sftp.exists(sftp_to_folder))
//             await sftp.mkdir(sftp_to_folder, false);
//         const datas = await sftp.list("upload/from");
//         _.forEach(datas, data => console.log(data));
//         myPool.release(sftp);
//     })
//     .catch((err) => {
//         // handle error - this is generally a timeout or maxWaitingClients
//         // error
//         console.log(err);
//     });

//myPool.drain().then(function () {
//     myPool.clear();
//});