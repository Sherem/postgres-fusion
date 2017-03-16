const Client = require('pg-native');
const Pool = require('pg').Pool;
const async = require('async');
const _ = require('lodash');
const sprintf = require('sprintf');
const elasticSearch = require('elasticsearch');

const GET_PROBE_INFO = 'get_probe_info';
const ADD_PROBE_INFO = 'add_probe_info';
const SET_ANALYTICS = 'set_analytics_';

var args = process.argv.slice(2);

var options = '';
var params = [];
var elasticClient;
var client;


args.forEach(a => {
    if (_.head(a) === '-') {
        options += a.slice(1);
    } else {
        params.push(a);
    }
});

var doCleanup = _.includes(options, 'c');

var esHost = params[0];

const call = cb => err => cb(err);

function log() {
    var args = Array.prototype.slice.call(arguments);
    var text = sprintf.apply(null, args);
    console.log(text);
}

function error() {
    var args = Array.prototype.slice.call(arguments);
    log.apply(null, args);
}

function getAanalyticsData(client, cb) {
    var retreived = 0;
    var total = -1;
    var pageSize = 100;
    var searchOpts = {
        index: 'analytics-*',
        query: {
            match_all: {}
        },
        size: pageSize,
        from: 0
    };

    async.whilst(() => total === -1 || retreived < total, function(done) {
        getPage(searchOpts, done);
    }, err => cb(err));

    function getPage(options, done) {
        async.waterfall([
            function(next) {
                elasticClient.search(searchOpts, next);
            },
            function(res, result, next) {
                if (res.hits) {
                    total = res.hits.total;
                    var len = res.hits.hits.length;

                    if (len) {
                        retreived += len;

                        options.from += len;

                        var pers = Math.round(retreived * 100 / total, 2)

                        log('%d%%', pers);

                        return insertDataChunk(client, res.hits.hits, next);
                    }

                }
                next(new Error('ENOENT', 'No idex data'));
            }
        ], (err) => done(err));
    }
}

function prepareInsertions(client, cb) {
    async.series([
        function(next) {
            client.prepare(ADD_PROBE_INFO,
                'INSERT INTO probe_Values (probe, valueName) VALUES ($1, $2)',
                2, call(next));
        },
        function(next) {
            client.prepare(GET_PROBE_INFO,
                'SELECT probe, valueName FROM probe_Values WHERE probe = $1',
                1, call(next));
        }
    ], call(cb));
}

/**
 * Creating database tables
 *
 * @param {Client} client
 * @param {Function} cb
 */
function createTables(client, cb) {
    var analyticsRequest =
        'CREATE TABLE IF NOT EXISTS ' +
        'probe_Values (' +
        '  id serial,' +
        '  probe varchar(25),' +
        '  valueName varchar(25),' +
        '  PRIMARY KEY (id)' +
        ');' +
        'CREATE INDEX IF NOT EXISTS pr_val ON probe_Values (probe, valueName)';

    client.query(analyticsRequest, call(cb));
}

function getStatementName(probe) {
    return SET_ANALYTICS + probe;
}

function newProbe(client, probe, values, cb) {
    var tableName = sprintf('analytics_%s', probe);
    var baseCreateRequest = 'CREATE TABLE IF NOT EXISTS %(table)s ' +
        '( id serial PRIMARY KEY, ' +
        '  time timestamp, ' +
        '  hostId varchar(50), ' +
        '  objectName  varchar(1024),' +
        '  %(valuesFields)s);' +
    'CREATE INDEX src_%(probe)s ON %(table)s ' +
    '(hostId, time, objectName)';

    var valuesFields = values.map(value => value + ' float8').join(',');

    var createRequest = sprintf(baseCreateRequest, {
        probe: probe, table: tableName, valuesFields: valuesFields
    });

    client.query(createRequest, call(cb));
}

function prepareProbeInsert(client, probe, values, cb) {
    var tableName = sprintf('analytics_%s', probe);

    var insertBase = 'INSERT INTO %(table)s ' +
        '(time, hostId, objectName, %(values)s) ' +
        'VALUES ($1, $2, $3, %(valueParams)s)';
    var insertRequest = sprintf(insertBase, {
        table: tableName,
        values: values.join(','),
        valueParams: values.map((v, i) => '$' + (i + 4)).join(',')
    });

    client.prepare(getStatementName(probe),
        insertRequest, values.length + 4,
        call(cb)
    );
}

const probesCache = {};

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

function getProbeInfo(client, metricRecord, cb) {
    var probe = metricRecord.probe;
    var values = _.keys(metricRecord.values);
    var cachedProbes = false;

    async.waterfall([
            function(next) {
                cachedProbes = probesCache[probe];
                if (cachedProbes) {
                    return next(null, probesCache[probe]);
                }
                client.execute(GET_PROBE_INFO, [probe], next);
            },
            function(probes, next) {
                if (_.isEmpty(probes)) {
                    return async.series([
                        function(next) {
                            startTransaction(client, next);
                        },
                        function(next) {
                            log('New probe: %s [%s]', probe, values.join(', '));
                            async.forEachSeries(values, function(value, done) {
                                client.execute(ADD_PROBE_INFO,
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
                    ], commitOrRollback(client, call(next)));
                } else {
                    probesCache[probe] = probes;
                    next();
                }
            },
            function(next) {
                if (cachedProbes) {
                    return next();
                }
                prepareProbeInsert(client, probe, values, next);
            }
        ],
        cb
    );
}

function insertValues(client, params, cb) {
    var probe = params.probe;
    var preparedData = [
        params.time,
        params.hostId,
        params.objectName
    ];
    var values = probesCache[probe];

    preparedData = values.reduce((preparedData, valueData) => {
        preparedData.push(params.values[valueData.valuename] || 0);
        return preparedData;
    }, preparedData);

    client.execute(getStatementName(probe), preparedData, call(cb));
}

function insertDataChunk(client, hitsArray, cb) {
    async.eachSeries(hitsArray, function(hit, done) {
        var source = hit._source;
        var typeArr = hit._type.split(':');
        var probe = typeArr.pop();
        var params = {
            probe: probe,
            time: source.timestamp,
            hostName: source.hostName,
            hostId: source.hostId,
            objectName: source.objectName,
            values: source.values
        };

        async.series([
            function(next) {
                getProbeInfo(client, params, next);
            },
            function(next) {
                insertValues(client, params, next);
            }
        ], done);
    }, cb);
}

function migrate(client, cb) {
    async.series([
        function(next) {
            createTables(client, next);
        },
        function(next) {
            prepareInsertions(client, next);
        },
        function(next) {
            getAanalyticsData(client, call(next));
        }
    ], cb);
}

function cleanup(client, cb) {
    log('Cleanup database');
    var cleanupRequest = 'DROP SCHEMA public CASCADE;' +
        'CREATE SCHEMA public;' +
        'GRANT ALL ON SCHEMA public TO postgres;' +
        'GRANT ALL ON SCHEMA public TO public;';
    client.query(cleanupRequest, call(cb));
}

function connectDb(noEsdb, cb) {
    if (!noEsdb) {
        var es = esHost.split(':');
        if (!es[1]) {
            es.push('9200');
        }
        elasticClient = new elasticSearch.Client({
            host: es.join(':'),
            log: 'error'
        });
    }

    client = new Client();
    client.connect(
        //'dbname=fusion user=fusion password=111',
//                'postgresql://fusion:Nexenta%4012@fu-100/mydb',
        'postgresql://fusion:Nexenta%4012@localhost/fusion',
        cb);
}

function usage() {
    log('Usage: app.js esdbIp[:port]');
    log('       app.js -c');
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
                return cleanup(client, next);
            }

            migrate(client, next);
        },
        function(next) {
            client.end(call(next));
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
