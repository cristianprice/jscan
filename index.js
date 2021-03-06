const cassandra = require('cassandra-driver');
const Mirror = require('./lib/mirror').Mirror;
const logger = require('./lib/logging');
const client = new cassandra.Client({
    contactPoints: ['10.11.1.131', '10.11.1.133'],
    /*contactPoints: ['10.11.5.62', '10.11.5.63'],*/
    keyspace: 'woow_backend'
});

const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database(':memory:');


client.on('log', (level, className, message, furtherInfo) => {
    logger.info(`[${level}] ${message}`);
});

const options = {
    'contactPoints': ['10.11.1.131', '10.11.1.133'],
    'keyspace': 'woow_backend',
    'tables': [{
        name: 'user_log',
        fetchSize: 500,
        indexes: [{
            'columns': ['time', 'actual_user'],
            'unique': false
        }],
        skipColumns: ['time']
    }, {
        name: 'accounts',
        fetchSize: 500
    }],
    'fetch': ['parallel', 'sequential'],
    'sqlPath': ':memory:',
    'interactive': true
};


client.connect().then(() => {

    let mirror = new Mirror({
        'cassandraClient': client,
        'keyspace': 'woow_backend',
        'tableName': 'user_log',
        'sqliteDb': db
    }, (err, data) => {
        logger.info(JSON.stringify(data));
    });

    mirror.withIndex(['time']).withIndex(['actual_user', 'time']).build();
});
