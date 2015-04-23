/**
 * @author Trilogis Srl
 * @author Gustavo German Soria
 *
 * Module in with parametrized queries are stored
 */

/**
 * Import of the line model
 * @type {exports}
 */
var line = require('./../models/line.js');

/**
 * Import of the road model
 * @type {exports}
 */
var road = require('./../models/road.js');

/**
 * Import of the polygon model
 * @type {exports}
 */
var polygon = require('./../models/polygon.js');

/**
 * Import of the point model
 * @type {exports}
 */
var point = require('./../models/point.js');

/**
 * Import of the validation module
 * @type {exports}
 */
var val = require('./../validation/validation.js');

/**
 * Import of the parameters module
 * @type {exports}
 */
var params = require('./../util/params.js');

/**
 * Internal use: it retrieves the correct table name (entity table) from the given entity type identifier stored in the callback object
 * @param callback Callback object in which is stored the entity type identifier
 * @returns {String} a table name
 */
var getTable = function(callback){

    /*
    validation of the entity type identifier
     */
    val.assertType(callback);

    /*
    variable for the table name, initialized as undefined
     */
    var table = undefined;

    /*
    select the correct value for the table variable
     */
    switch (callback.params.type) {
        case params.LINE :
        {
            table = line.model.table;
        }
            break;
        case params.POLYGON :
        {
            table = polygon.model.table;
        }
            break;
        case params.ROAD :
        {
            table = road.model.table;
        }
            break;
        case params.POINT :
        {
            table = point.model.table;
        }
            break;
    }

    /*
    validation of the table variable
     */
    val.assertNotUndefined(table);

    return table;
}

/**
 * Internal use: it retrieves the correct table name (style table) from the given entity type identifier stored in the callback object
 * @param callback Callback object in which is stored the entity type identifier
 * @returns {String} a table name
 */
var getStyleTable = function(callback){

    /*
     validation of the entity type identifier
     */
    val.assertType(callback);

    /*
     variable for the table name, initialized as undefined
     */
    var table = undefined;

    /*
     select the correct value for the table variable
     */
    switch (callback.params.type) {
        case params.LINE :
        {
            table = line.model.styleTable;
        }
            break;
        case params.POLYGON :
        {
            table = polygon.model.styleTable;
        }
            break;
        case params.ROAD :
        {
            table = road.model.styleTable;
        }
            break;
        case params.POINT :
        {
            table = point.model.styleTable;
        }
            break;
    }

    /*
     validation of the table variable
     */
    val.assertNotUndefined(table);

    return table;
}

/**
 * Query used to retrieve all the distinct identifiers of the records to insert/update from the temporary table
 * @param callback Callback object in which is stored the entity type identifier
 * @returns {string} the query string
 */
var getIds = function(callback){

    /**
     * Table name. It is retrieved from the entity type identifier stored in the callback object
     * @type {String} the table name
     */
    var table = getTable(callback);

    return "SELECT DISTINCT osm_id FROM "+table;
}

/**
 * Query used to retrieve the list of record with the given osm identifier
 *
 * It requires:
 * $1::bigint -> the osm identifier
 *
 * @param callback Callback object in which is stored the osm identifier
 * @returns {string} the query string
 */
var getById = function(callback){

    /**
     * Table name. It is retrieved from the entity type identifier stored in the callback object
     * @type {String} the table name
     */
    var table = getTable(callback);

    return "SELECT * FROM "+table+" WHERE osm_id = $1::bigint";
}

/**
 * Query used to apply the correct type identifier
 * @param callback Callback object in which is stored the property key
 *
 * It requires:
 * $1::integer -> the type identifier
 * $2::varchar -> the property value
 *
 * @param key {string} the property key
 * @returns {string} the query string
 */
var applyTypeId = function(callback, key){
    /**
     * Table name. It is retrieved from the entity type identifier stored in the callback object
     * @type {String} the table name
     */
    var table = getTable(callback);

    return 'UPDATE ' + table + ' SET type_id = $1::integer WHERE "' + key.replace(/\s+/g, '') + '" = $2::varchar';
}

/**
 * Query used to apply the soft delete
 *
 * It requires:
 * $1::bigint -> the osm identifier
 *
 * @param callback Callback object in which is stored the osm identifier
 * @returns {string} the query string
 */
var softDelete = function(callback){

    /**
     * Table name. It is retrieved from the entity type identifier stored in the callback object
     * @type {String} the table name
     */
    var table = getTable(callback);
    return "UPDATE " + table + " SET is_deleted = true WHERE osm_id = $1::bigint";
}

/**
 * Query used to retrieve the number of records to insert/update
 *
 * @param callback Callback object in which is stored the entity type identifier
 * @returns {string} the query string
 */
var countEntries = function(callback){

    /**
     * Table name. It is retrieved from the entity type identifier stored in the callback object
     * @type {String} the table name
     */
    var table = getTable(callback);
    return "SELECT count(osm_id) as osm_id FROM "+table;
}

/**
 * Query used to retrieve the list of properties. The correct table is selected using the entity type identifier
 * within the callback object
 *
 * @param callback Callback object which contains the entity type identifier
 * @returns {string} the query string
 */
var getProperties = function(callback){

    /**
     * Table name. It is retrieved from the entity type identifier stored in the callback object
     * @type {String} the table name
     */
    var table = getStyleTable(callback);
    return "SELECT type_id, type_key, type_value FROM "+table;
}

/**
 * Query used to insert a record.
 *
 * @param callback Callback object which contains the entity type identifier
 * @param fields {string} a string which represents the list of fields
 * @param values {string} a string which represents the list of values
 * @returns {string} the query string
 */
var insert = function(callback, fields, values){

    /*
    validation of the list of fields and values
     */
    val.assertInsert(fields, values);

    /**
     * Table name. It is retrieved from the entity type identifier stored in the callback object
     * @type {String} the table name
     */
    var table = getTable(callback);

    return "INSERT INTO " + table + " ("+fields+") VALUES ("+values+")";
}

module.exports.getById = getById;
module.exports.getIds = getIds;
module.exports.applyTypeId = applyTypeId;
module.exports.softDelete = softDelete;
module.exports.countEntries = countEntries;
module.exports.getProperties = getProperties;
module.exports.insert = insert;