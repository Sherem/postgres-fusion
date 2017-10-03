const Client = require('pg-native');
const Pool = require('pg').Pool;
const async = require('async');
const _ = require('lodash');
const sprintf = require('sprintf');
const elasticSearch = require('elasticsearch');

const args = process.argv.slice(2);

let options = '';
const params = [];
let elasticClient;
let pool;

args.forEach(a => {
    if (_.head(a) === '-') {
        options += a.slice(1);
    } else {
        params.push(a);
    }
});

let doCleanup = _.includes(options, 'c');

let esHost = params[0];

const call = cb => err => cb(err);

const callRelease = (cb, release) => () => {
    release();
    cb();
};

function log() {
    const args = Array.prototype.slice.call(arguments);
    const text = sprintf.apply(null, args);
    process.stdout.write(text);
    process.stdout.write(text);
}

function logLn() {
    const args = Array.prototype.slice.call(arguments);
    const text = sprintf.apply(null, args);
    console.log(text);
}

function error() {
    const args = Array.prototype.slice.call(arguments);
    logLn.apply(null, args);
}

function insertProbes(pool, cb) {
    const searchOpts = {
        index: 'analytics-*',
        query: {
            match_all: {}
        },
        size: 1000
    };

    getEsdbData(searchOpts, function(hits, done) {
        async.each(hits, function(hit, done) {
            const source = hit._source;
            const typeArr = hit._type.split(':');
            const probe = typeArr.pop();
            const params = {
                probe: probe,
                time: source.timestamp,
                hostName: source.hostName,
                hostId: source.hostId,
                objectName: source.objectName,
                values: source.values
            };

            async.parallel([
                function(doneAnalytic) {
                    async.waterfall([
                        function(next) {
                            pool.connect(next);
                        },
                        function(client, release, next) {
                            async.series([
                                function(next) {
                                    getProbeInfo(client, params, next);
                                },
                                function(next) {
                                    insertValues(client, params, next);
                                }
                            ], (err) => {
                                release();
                                next(err);
                            });
                        }
                    ], doneAnalytic);
                },
                function(doneHosts) {
                    insertHosts(pool, {
                        hostName: params.hostName,
                        hostId: params.hostId
                    }, doneHosts);
                }
            ], done);
        }, done);
    }, cb);
}

function insertLog(pool, cb) {
    const searchOpts = {
        index: 'logs',
        query: {
            match_all: {}
        },
        size: 100
    };

    getEsdbData(searchOpts, function(hits, done) {
        async.forEach(hits,
            function(hit, done) {
                if (hit._type !== 'hostLog') {
                    logLn('Unsupported type: %s', hit._type);
                    return done();
                }
                const pl = hit._source;
                const preparedData = [
                    pl.hostId,
                    pl.component,
                    pl.message,
                    pl.facility,
                    pl.severity,
                    pl.timestamp,
                    pl.origin
                ];

                const request = 'INSERT INTO hostLog' +
                    '(' +
                    '  hostId,' +
                    '  component,' +
                    '  message,' +
                    '  facility,' +
                    '  severity,' +
                    '  time,' +
                    '  origin' +
                    ') values ($1, $2, $3, $4, $5, $6, $7)';

                async.parallel([
                    function(doneLog) {
                        async.waterfall([
                            function(next) {
                                pool.connect(next);
                            },
                            function(client, release, next) {
                                client.query(request, preparedData, err => {
                                    release();
                                    next(err);
                                });
                            }
                        ], doneLog);
                    },
                    function(doneHost) {
                        insertHosts(pool, {
                            hostName: pl.hostname,
                            hostId: pl.hostId
                        }, doneHost);
                    }
                ], done);

            }, done);
    }, cb);
}

function getEsdbData(search, insertChunk, cb) {
    let retrieved = 0;
    let total = -1;

    const searchOpts = _.cloneDeep(search);
    searchOpts.size = searchOpts.size || 100;
    searchOpts.from = 0;
    let len;

    const avgTs = Date.now();
    let ts;

    async.whilst(() => total === -1 || retrieved < total, function(done) {
        getPage(searchOpts, done);
    }, err => cb(err));

    function getPage(options, done) {
        async.waterfall([
            function(next) {
                ts = Date.now();
                elasticClient.search(searchOpts, next);
            },
            function(res, result, next) {
                if (res.hits) {
                    total = res.hits.total;
                    len = res.hits.hits.length;

                    if (len) {
                        retrieved += len;

                        options.from += len;

                        return insertChunk(res.hits.hits, next);
                    }

                }
                next(new Error('ENOENT', 'No idex data'));
            },
            function(next) {
                const spend = Date.now() - ts;
                const avgSpend = Date.now() - avgTs;

                const instantSpeed = len / spend;
                const avgSpeed = retrieved / avgSpend;

                const pers = retrieved * 100 / total;

                const estimated = Date.now() + (total - retrieved) * avgSpeed;
                const finish = new Date(estimated);

                log('   %5.2f%%  %8.0f rec/sec %8.0f rec/sec finish at: %s\r',
                    pers, instantSpeed * 1000, avgSpeed * 1000, finish);
                next();
            }
        ], (err) => done(err));
    }
}

const hostsCache = {};

function insertHosts(pool, params, cb) {
    const hostId = params.hostId;
    if (hostsCache[hostId]) {
        return cb();
    }

    logLn('New host: %(hostName)s %(hostId)s', params);

    async.waterfall([
        function(next) {
            pool.connect(next);
        },
        function(client, release, next) {
            async.waterfall([
                function(next) {
                    client.query('SELECT hostid FROM hosts WHERE hostid=$1',
                        [hostId], next);
                },
                function(hosts, next) {
                    if (_.isEmpty(hosts)) {
                        client.query('INSERT INTO hosts (hostid, hostname)' +
                            'values ($1,$2) ON CONFLICT (hostid) DO NOTHING',
                            [hostId, params.hostName], call(next));
                    } else {
                        next();
                    }
                },
                function(next) {
                    hostsCache[hostId] = true;
                    next();
                }
            ], callRelease(next, release));
        }
    ], cb);
}

/**
 * Creating database tables
 *
 * @param {Pool} pool
 * @param {Function} cb
 */
function createTables(pool, cb) {
    const requests = [
        'CREATE TABLE IF NOT EXISTS ' +
        'probe_Values (' +
        '  id serial,' +
        '  probe varchar(25),' +
        '  valueName varchar(25),' +
        '  PRIMARY KEY (id)' +
        ');' +
        'CREATE INDEX IF NOT EXISTS pr_val ON probe_Values (probe, valueName)',

        'CREATE TABLE IF NOT EXISTS ' +
        'hostLog (' +
        '  id serial PRIMARY KEY,' +
        '  hostId varchar(50),' +
        '  component varchar(50),' +
        '  message varchar(1024),' +
        '  facility varchar(25),' +
        '  severity varchar(15),' +
        '  time timestamp,' +
        '  origin varchar(15)' +
        ');' +
        'CREATE INDEX IF NOT EXISTS host_log ON hostLog (hostid, severity)',

        'CREATE TABLE IF NOT EXISTS ' +
        'hosts (' +
        '  hostId varchar(20) PRIMARY KEY,' +
        '  hostName varchar(20)' +
        ')'

    ];

    async.each(requests, function(request, done) {
        async.waterfall([
            function(next) {
                pool.connect(next);
            },
            function(client, done, next) {
                client.query(request, err => {
                    done();
                    next(err);
                });
            }
        ], done);
    }, cb);

}

function newProbe(client, probe, values, cb) {
    const tableName = sprintf('analytics_%s', probe);
    const baseCreateRequest = 'CREATE TABLE IF NOT EXISTS %(table)s ' +
        '( id serial PRIMARY KEY, ' +
        '  time timestamp, ' +
        '  hostId varchar(50), ' +
        '  objectName  varchar(1024),' +
        '  %(valuesFields)s);' +
        'CREATE INDEX IF NOT EXISTS src_%(probe)s ON %(table)s ' +
        '(hostId, time, objectName)';

    const valuesFields = values.map(value => value + ' float8').join(',');

    const createRequest = sprintf(baseCreateRequest, {
        probe: probe, table: tableName, valuesFields: valuesFields
    });

    client.query(createRequest, call(cb));
}

const probesCache = {};
const probeInsertions = {};

function prepareProbeInsert(probe, values) {
    const tableName = sprintf('analytics_%s', probe);

    const insertBase = 'INSERT INTO %(table)s ' +
        '(time, hostId, objectName, %(values)s) ' +
        'VALUES ($1, $2, $3, %(valueParams)s)';

    probeInsertions[probe] = sprintf(insertBase, {
        table: tableName,
        values: values.join(','),
        valueParams: values.map((v, i) => '$' + (i + 4)).join(',')
    });

}

function startTransaction(client, cb) {
    client.query('BEGIN;', call(cb));
}

function commitOrRollback(client, cb) {
    return function(err, result) {
        if (err) {
            return cb(err);
        }

        client.query('COMMIT;', function(err) {
            if (err) {
                return cb(err);
            }

            if (result) {
                return cb(null, result);
            }

            cb();
        });
    };
}

const probeCreating = {};

function getProbeInfo(client, metricRecord, cb) {
    const probe = metricRecord.probe;
    const values = _.keys(metricRecord.values);

    if (probeCreating[probe]) {
        return async.whilst(
            () => probeCreating[probe], done => setTimeout(done, 10),
            function(err) {
                if (err) {
                    return cb;
                }

                getProbeInfo(client, metricRecord, cb);
            });
    }

    const cachedProbes = probesCache[probe];
    if (cachedProbes) {
        return cb();
    }

    probeCreating[probe] = true;

    prepareProbeInsert(probe, values);

    async.waterfall([
            function(next) {
                startTransaction(client, next);
            },
            function(next) {
                client.query('SELECT probe, valueName FROM probe_Values' +
                    ' WHERE probe = $1', [probe], next);
            },
            function(probes, next) {
                if (_.isEmpty(probes)) {
                    return async.series([
                        function(next) {
                            logLn('New probe: %s [%s]',
                                probe, values.join(', '));
                            async.forEachSeries(values, function(value, done) {
                                client.query(
                                    'INSERT INTO probe_Values ' +
                                    '(probe, valueName) ' +
                                    'VALUES ($1, $2)',
                                    [probe, value], call(done));
                            }, next);
                        },
                        function(next) {
                            probesCache[probe] = values.map(val => ({
                                probe: probe,
                                valuename: val
                            }));
                            newProbe(
                                client, probe, values, next
                            );
                        }
                    ], next);
                } else {
                    probesCache[probe] = probes;
                    next();
                }
            }
        ],
        commitOrRollback(client, err => {
            probeCreating[probe] = false;
            cb(err);
        }));
}

function insertValues(client, params, cb) {
    const probe = params.probe;
    let preparedData = [
        params.time,
        params.hostId,
        params.objectName
    ];
    const values = probesCache[probe];

    preparedData = values.reduce((preparedData, valueData) => {
        preparedData.push(params.values[valueData.valuename] || 0);
        return preparedData;
    }, preparedData);

    client.query(probeInsertions[probe], preparedData, function(err) {
        if (err) {
            logLn(JSON.stringify(params, null, '  '));
            cb(err);
        }

        cb(null);
    });
}

function migrate(pool, cb) {
    async.series([
        function(next) {
            createTables(pool, next);
        },
        function(next) {
            async.parallel([
                function(next) {
                    insertLog(pool, next);
                },
                function(next) {
                    insertProbes(pool, next);
                }
            ], next);
        }

    ], cb);
}

function cleanup(pool, cb) {
    logLn('Cleanup database');
    const cleanupRequest = 'DROP SCHEMA public CASCADE;' +
        'CREATE SCHEMA public;' +
        'GRANT ALL ON SCHEMA public TO postgres;' +
        'GRANT ALL ON SCHEMA public TO public;';

    async.waterfall([
        function(next) {
            pool.connect(next);
        },
        function(client, release, next) {
            client.query(cleanupRequest, err => {
                release();
                next(err);
            });
        }
    ], cb);
}

function connectDb(noEsdb, cb) {
    if (!noEsdb) {
        const es = esHost.split(':');
        if (!es[1]) {
            es.push('9200');
        }
        elasticClient = new elasticSearch.Client({
            host: es.join(':'),
            log: 'error'
        });
    }

    pool = new Pool({
        // user: 'fusion',
        // password: 'Nexenta@12',
        // host: 'localhost',
        dtatbase: 'fusion',
        Client: Client,
        max: 30, //set pool max size to 30
        min: 4, //set min pool size to 4
        idleTimeoutMillis: 1000 //close idle clients after 1 second
    });

    cb();

}

function usage() {
    logLn('Usage: app.js esdbIp[:port]');
    logLn('       app.js -c');
}

function main() {

    if (!doCleanup && !esHost) {
        usage();
        process.exit(1);
        return;
    }

    async.series([
        function(next) {
            connectDb(doCleanup, next);
        },
        function(next) {
            if (doCleanup) {
                return cleanup(pool, next);
            }

            migrate(pool, next);
        }
    ], function(err) {
        if (err) {
            error(err.toString());
            process.exit(1);
        }

        process.exit(0);
    });
}

main();
