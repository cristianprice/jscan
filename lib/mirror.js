const cassandra = require('cassandra-driver');
const dataTypes = cassandra.types.dataTypes;

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

    console.warn(`Unsupported type code ${type}. Converting to text.`);
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

    function withIndex(table, column, name, unique) {
        idxCount++;
        indexes.push({
            'table': table,
            'column': column,
            'name': name || 'IDX_ON_COL_' + idxCount,
            'unique': unique || false
        });

        return _this;
    }

    this.build = function() {
        console.log(`Mirroring table ${keyspace}.${tableName}`);
        cassandraClient.metadata.getTable(keyspace, tableName, (err, tableInfo) => {
            if (err) {
                callback(err);
                return;
            }

            console.log(`Searching for partition keys.`);
            for (let pkIdx in tableInfo.partitionKeys) {
                let pk = tableInfo.partitionKeys[pkIdx];

                console.log(`Found partition key: ${pkIdx} ${pk.name}`);
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

            console.log(`Found columns: ${JSON.stringify(columns)}`);
            callback(null, columns);
        });
    };
}

function sqliteCreateTable(options) {

}

module.exports.Mirror = Mirror;
