const Sequelize = require("sequelize");
const config = require('./config');
const sequelize = new Sequelize(config.DB_NAME, config.DB_USERNAME, config.DB_PASSWORD, {
    host: config.DB_HOST,
    dialect: 'mysql',
    operatorsAliases: false,
    port: 3306,
    pool: {
        max: 5,
        min: 0,
        acquire: 30000,
        idle: 10000
    },
    define: {
        timestamps: false
    },
    dialectOptions: {
        encrypt: false
    },
    logging: false,
});

var db = {
    Sequelize: Sequelize,
    sequelize: sequelize
};
db.files = db.sequelize.import('./models/files');
module.exports = db;