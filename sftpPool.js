const genericPool = require("generic-pool");
const Client = require("ssh2-sftp-client");
const config = require("./config");
const _ = require("lodash");

const sftp_config = {
    host: config.SFTP_HOST,
    port: config.SFTP_PORT,
    username: config.SFTP_USERNAME,
    password: config.SFTP_PASSWORD
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
    min: 3 // minimum size of the pool
};

const myPool = genericPool.createPool(factory, opts);

module.exports = myPool;

// const resourcePromise = myPool.acquire();

// resourcePromise
//     .then(async (client) => {
//         const datas = await client.list("upload/from");
//         _.forEach(datas, data => console.log(data));
//         myPool.release(client);
//     })
//     .catch((err) => {
//         // handle error - this is generally a timeout or maxWaitingClients
//         // error
//         console.log(err);
//     });

// myPool.drain().then(function() {
//     myPool.clear();
//     });