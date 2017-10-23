const cassandra = require('cassandra-driver');
const Mirror = require('./lib/mirror').Mirror;
const client = new cassandra.Client({
    contactPoints: ['10.11.1.131', '10.11.1.133'],
    /*contactPoints: ['10.11.5.62', '10.11.5.63'],*/
    keyspace: 'woow_backend'
});

client.on('log', (level, className, message, furtherInfo) => {
    //console.log(`[${level}] ${message}`);
});

client.connect().then(() => {

    let mirror = new Mirror({
        'cassandraClient': client,
        'keyspace': 'woow_backend',
        'tableName': 'user_log',
        'sqliteDb': 'sqliteDb'
    });

    mirror.build();
});
