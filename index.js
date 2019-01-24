'use strict';
const express = require('express');
const app = express();
// const rp = require('request-promise');
const bodyParser = require('body-parser')
const expressWinston = require('express-winston');
const winston = require('winston');
const sftp_to_azure_blob_with_bus = require('./sftp_to_azure_blob_with_bus');
const schedule = require("node-schedule");
const sftp_to_azure_blob_with_bus_instance = new sftp_to_azure_blob_with_bus();
const db = require("./dbPool");

schedule.scheduleJob("*/1 * * * *", date => {
  console.log(`${date} - Scheduler - Receiver Invoked`);
  sftp_to_azure_blob_with_bus_instance.recieveMessageFromQueue();
});

app.use(expressWinston.logger({
    transports: [
        new winston.transports.Console({
            json: true,
            colorize: true
        })
    ]
}));

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: true }))
// app.use(express.static('src/dist/'));

// app.get('/', function (request, response) {
//     response.redirect('index.html');
// });

app.get('/api/sftp/initiate', async (request, response) => {
    await sftp_to_azure_blob_with_bus_instance.start();
    response.json({ status: "initiated" });
});

app.get('/api/sftp/files/:offset', async (request, response) => {
    let files = await db.files.findAll({
        raw: true,
        offset: parseInt(request.params.offset), limit: 10
      });
      response.json(files);
});

app.get('*', function (req, res) {
    res.sendStatus(404);
});

const port = process.env.PORT || 3200;
app.listen(port, function () {
    console.log(`Server listening on port ${port}`);
});

module.exports = app;