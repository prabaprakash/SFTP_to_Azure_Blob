/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('files', {
    id: {
      type: DataTypes.INTEGER(11),
      allowNull: false,
      primaryKey: true,
      autoIncrement: true,
    },
    name: {
      type: DataTypes.STRING(100),
      allowNull: false
    },
    status: {
      type: DataTypes.STRING(50),
      allowNull: false
    },
    url: {
      type: DataTypes.STRING(200),
      allowNull: false
    },
    created_at: {
      type: DataTypes.DATE,
      allowNull: false
    },
    updated_at: {
      type: DataTypes.DATE,
      allowNull: false
    },
    size: {
      type: DataTypes.DECIMAL,
      allowNull: true
    },
    modifyTime: {
      type: DataTypes.STRING(20),
      allowNull: true
    }
  }, {
    tableName: 'files'
  });
};
