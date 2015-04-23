/**
 * @author Trilogis Srl
 * @author Gustavo German Soria
 *
 * Database Module
 */

/**
 * Parameters import
 * @type {exports}
 */
var params= require('./../util/params.js');

/**
 * PostgreSQL client for node.js module import.
 * @type {exports}
 */
var pg = require('pg');

/**
 * Username for the main database
 * @type {string}
 */
var gisDatabaseUser = 'gustavosoria';

/**
 * Name of the main database
 * @type {string}
 */
var gisDatabaseName = 'gis';

/**
 * Address of the main database
 * @type {string}
 */
var gisDatabaseAddress = 'localhost';

/**
 * Username for the temporary database
 * @type {string}
 */
var tempDatabaseUser = 'gustavosoria';

/**
 * Name of the temporary database
 * @type {string}
 */
var tempDatabaseName = 'gis_temp';

/**
 * Address of the temporary database
 * @type {string}
 */
var tempDatabaseAddress = 'localhost';

/**
 * Connection string for the main database
 * @type {string}
 */
var gisDatabase = "postgres://"+gisDatabaseUser+":@"+gisDatabaseAddress+"/"+gisDatabaseName+"";

/**
 * Connection string for the temporary database
 * @type {string}
 */
var tempDatabase = "postgres://"+tempDatabaseUser+":@"+tempDatabaseAddress+"/"+tempDatabaseName+"";

/**
 * Short link for the variable of the main database
 * @type {exports.GIS|*}
 */
var GIS = params.GIS;

/**
 * Short link for the variable of the temporary database
 * @type {exports.TEMP|*}
 */
var TEMP = params.TEMP;

/**
 * Execute the query on the given database
 * @param database Database variable in which the query has been executed
 * @param query Query string
 * @param params Parameters for the query
 * @param callback Callback object
 */
var executeQuery = function(database, query, params, callback){

    /**
     * The connection string is inited according to the given database
     * @type {string}
     */
    var connString = (database === GIS) ? gisDatabase : tempDatabase;

    /*
    connection
     */
    pg.connect(connString, function(err, client, done) {
        if(err) {
            return console.error('error fetching client from pool', err);
        }
        client.query(query, params, function(err, result) {
            done();
            if(err) {
                if (callback && callback.onError){
                    callback.onError(callback);
                }
//                return console.error('error running query\n'+query+"\n"+params+"\n", err);
            } else {
                /*
                At this step the query has been correctly executed
                and the result is stored in result.rows.

                The result is stored also in the callback object
                [callback.rows]
                in order to be used in the process flow.

                Then, the next method is called.
                 */
                if (callback && callback.list) {
                    var _next = callback.list.pop();
                    if (_next) {
                        callback.rows = result.rows;
                        _next(callback);
                    }
                }
            }
        });
    });
}


module.exports.execute = executeQuery;
