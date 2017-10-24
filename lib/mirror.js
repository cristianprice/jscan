const cassandra = require('cassandra-driver');
const dataTypes = cassandra.types.dataTypes;
const logger = require('./logging');

function typeConvert(type) {
    switch (type) {
        case dataTypes.int:
        case dataTypes.bigint:
        case dataTypes.varint:
        case dataTypes.smallint:
        case dataTypes.tinyint:
        case dataTypes.boolean:
            return 'INT';

        case dataTypes.timestamp:
        case dataTypes.decimal:
            return 'NUMERIC';

        case dataTypes.blob:
            return 'BLOB';

        case dataTypes.list:
        case dataTypes.set:
        case dataTypes.udt:
        case dataTypes.map:
        case dataTypes.tuple:
            return 'JSON';

        case dataTypes.double:
        case dataTypes.float:
            return 'REAL';
    }

    logger.warn(`Unsupported type code ${type}. Converting to text.`);
    return 'TEXT';
}


function Mirror(options, callback) {

    let _this = this;
    let idxCount = 0;

    let cassandraClient = options.cassandraClient;
    let keyspace = options.keyspace;
    let tableName = options.tableName;
    let sqliteDb = options.sqliteDb;
    callback = callback || function() {};

    let indexes = [];
    let columns = {};

    if (!cassandraClient || !keyspace || !tableName || !sqliteDb) {
        callback(new Error(`Error. Params not set ${cassandraClient} ${keyspace} ${tableName} ${sqliteDb}`));
        return;
    }

    this.withIndex = function(columns, name, unique) {
        idxCount++;
        let idx = {
            'columns': columns,
            'name': name || 'IDX_ON_COL_' + idxCount,
            'unique': unique || false
        };

        indexes.push(idx);
        logger.info(`Added index ${JSON.stringify(idx)}`);
        return _this;
    };

    this.build = function() {
        logger.info(`Mirroring table ${keyspace}.${tableName}`);
        cassandraClient.metadata.getTable(keyspace, tableName, (err, tableInfo) => {
            if (err) {
                callback(err);
                return;
            }

            logger.info(`Searching for partition keys.`);
            for (let pkIdx in tableInfo.partitionKeys) {
                let pk = tableInfo.partitionKeys[pkIdx];

                logger.info(`Found partition key: ${pkIdx} ${pk.name}`);
                columns[pk.name] = {
                    pk: true,
                    pkIdx: pkIdx
                };
            }

            for (let colName in tableInfo.columnsByName) {
                let col = columns[colName] || {};
                col.type = typeConvert(tableInfo.columnsByName[colName].type.code);
                columns[colName] = col;
            }

            logger.info(`Found columns: ${JSON.stringify(columns)}`);
            sqliteCreateTable({
                'tableName': tableName,
                'columns': columns,
                'indexes': indexes,
                'sqliteDb': sqliteDb
            }, callback);
        });
    };
}

function sqliteCreateTable(data, callback) {
    let sqliteDb = data.sqliteDb;
    let tableStr = makeTableString(data);
    let idxStrings = makeIndexesStrs(data);
    logger.info(`Creating table \n${tableStr}`);
    logger.info(`Creating indexes \n${idxStrings}`);

    sqliteDb.serialize(function() {

        let err;
        try {
            sqliteDb.run(tableStr);
            for (let idx in idxStrings) {
                sqliteDb.run(idxStrings[idx]);
            }
        } catch (e) {
            err = e;
        }

        callback(err, data);
    });
}

function makeIndexesStrs(data) {
    let ret = [];

    for (let idx in data.indexes) {
        let idxData = data.indexes[idx];
        let idxStr = `CREATE ${idxData.unique?'UNIQUE':''} INDEX ${idxData.name} ON ${data.tableName}(${idxData.columns.join()})`;
        ret.push(idxStr);
    }

    return ret;
}

function makeTableString(data) {
    let columns = data.columns;
    let indexes = data.indexes;

    let pks = [];

    let ret = `CREATE TABLE ${data.tableName} (`;
    for (let colName in columns) {
        let colInfo = columns[colName];
        ret += `${colName} ${colInfo.type},\n`;
        if (colInfo.pk) {
            pks.push(colName);
        }
    }

    ret += `PRIMARY KEY (${pks.join()}) )`;
    return ret;
}

module.exports.Mirror = Mirror;
