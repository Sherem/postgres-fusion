const path = require('path');

const FUSION_PATH = process.env.FUSION_PATH || path.join(process.env.HOME,
        'fusion');

const nefEnvironmentLoader =
    require(path.join(FUSION_PATH, 'nefEnvironmentLoader'));

process.env.NEF_ENV = 'development';

nefEnvironmentLoader.load(false);

var splitedEnv = process.env.NODE_PATH.split(':');

splitedEnv.push(FUSION_PATH);
splitedEnv.push(path.join(FUSION_PATH, 'lib'));

process.env['NODE_PATH'] = splitedEnv.join(':');

console.log('NODE_PATH=' + process.env.NODE_PATH);

const Client = require('pg-native');
const async = require('async');

const NefError = require('nef/error').NefError;
const elasticClient = require('lib/elasticsearchClient');

const INSERT_METRIC_REQUEST = 'insert_request';

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

                        console.log(Math.round(retreived * 100 / total, 2) +
                            '%\r');
                        return insertDataChunk(client, res.hits.hits, next);
                    }

                }
                next(new NefError('ENOENT', 'No idex data'));
            }
        ], (err) => done(err));
    }
}

function prepareInsertions(client, cb) {
    client.prepare(INSERT_METRIC_REQUEST,
        'INSERT INTO analytics ' +
        '(metric, time, hostName, hostId, objectName, values) ' +
        'VALUES' +
        '($1,$2,$3,$4,$5,$6::jsonb)', 6, err => cb(err));
}

function insertDataChunk(client, hitsArray, cb) {
    async.eachSeries(hitsArray, function(hit, done) {
        var source = hit._source;
        var params = [
            hit._type,
            source.timestamp,
            source.hostName,
            source.hostId,
            source.objectName,
            JSON.stringify(source.values)
        ];
        client.execute(INSERT_METRIC_REQUEST, params, (err, rows) => {
            if (err) {
                return done(err);
            }
            done(err);
        });
    }, cb);
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
        'analytics (' +
        '  id serial,' +
        '  metric varchar(50),' +
        '  time timestamp,' +
        '  hostName varchar(50),' +
        '  hostId varchar(50),' +
        '  objectName varchar(50),' +
        '  values jsonb, ' +
        '  PRIMARY KEY (id)' +
        ');';

    client.query(analyticsRequest, err => cb(err));
}

function main() {
    const client = new Client();

    async.waterfall([
        function(next) {
            elasticClient.whenReady().then(() => next());
        },
        function(next) {
            client.connect(
                'postgresql://fusion:Nexenta%4012@fu-100/mydb', next);
        },
        function(next) {
            createTables(client, next);
        },
        function(next) {
            prepareInsertions(client, next);
        },
        function(next) {
            next();
            //getAanalyticsData(client, next);
        },
        function(res, next) {
            client.end(err => next(err));
        }
    ], function(err) {
        if (err) {
            console.log(err.toString());
            process.exit(1);
        }

        process.exit(0);
    });
}

main();
