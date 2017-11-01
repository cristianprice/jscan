const Mirror = require('./mirror').Mirror;
const logger = require('./logging');
const cassandra = require('cassandra-driver');
const sqlite3 = require('sqlite3').verbose();


function Scanner(options) {

    let _this = this;

    //cassandra contactPoints
    let contactPoints = options.contactPoints;
    let fetchType = options.fetchType; //how to scan tables ['parallel', 'sequential'],
    let dataPath = options.dataPath || ':memory:'; //sqlite data path
    let interactive = options.interactive;
    let defaultKeyspace = options.defaultKeyspace;

    let client = new cassandra.Client({
        contactPoints: contactPoints,
        keyspace: defaultKeyspace
    });

    let sqlite3 = require('sqlite3').verbose();
    let sqliteDb = new sqlite3.Database(dataPath);

    let tableScans = [];

    this.addTableScan = function (tableName, keyspace, fetchSize, indexes, skipColumns) {
        tableScans.push({
            'tableName': tableName,
            'keyspace': keyspace || defaultKeyspace,
            'fetchSize': fetchSize || 500,
            'indexes': indexes || [],
            'skipColumns': skipColumns || []
        });

        return _this;
    };

    this.scan = function (callback) {
        let tableScanLen = tableScans.length;
        if (tableScanLen <= 0) {
            callback(new Error('No tables added.'));
        }

        for (let tableScan in tableScans) {
            let tableInfo = tableScans[tableScan];
            let mirror = new Mirror({
                'cassandraClient': client,
                'keyspace': defaultKeyspace,
                'tableName': 'user_log',
                'sqliteDb': sqliteDb
            }, (err, data) => {
                logger.info(JSON.stringify(data));
            });
        }
    };
}