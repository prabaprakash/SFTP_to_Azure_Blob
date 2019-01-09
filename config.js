const config = {
    SFTP_HOST: process.env.SFTP_HOST,
    SFTP_PORT: process.env.SFTP_PORT,
    SFTP_USERNAME: process.env.SFTP_USERNAME,
    SFTP_PASSWORD: process.env.SFTP_PASSWORD,
    AZURE_STORAGE_CONNECTION_STRING: process.env.AZURE_STORAGE_CONNECTION_STRING,
    AZURE_SERVICEBUS_CONNECTION_STRING: process.env.AZURE_SERVICEBUS_CONNECTION_STRING,
    DB_HOST: process.env.DB_HOST,
    DB_USERNAME: process.env.DB_USERNAME,
    DB_PASSWORD: process.env.DB_PASSWORD,
    DB_NAME: process.env.DB_NAME,
}
module.exports = config;