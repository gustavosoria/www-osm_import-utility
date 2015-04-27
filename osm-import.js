/**
 * @author Trilogis Srl
 * @author Gustavo German Soria
 *
 * Import module
 */

var namesController = require('./controllers/names-controller');
/**
 * Import of the query definitions module
 * @type {exports}
 */
var queries = require('./queries/queries.js');

/**
 * Import of the log module
 * @type {exports}
 */
var log = require('./util/debug.js');

/**
 * Import of the params module
 * @type {exports}
 */
var params= require('./util/params.js');

/**
 * Import of the module which contains utilities for the callback object
 * @type {exports}
 */
var cbu = require('./util/callback-util.js');

/**
 * Import of the validation module
 * @type {exports}
 */
var val = require('./validation/validation.js');

/**
 * Import of the database module
 * @type {exports}
 */
var database = require('./db/db.js');

/**
 * It represent the current entity type used for the import process
 * @type {number}
 */
var current = undefined;

/**
 * It represents the number of records to store in the main database for the 'current' entity
 * @type {number}
 */
var counter = undefined;

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

var getNames = function(callback){
    var query = "SELECT osm_id, name FROM planet_osm_line WHERE name IS NOT NULL";
    database.execute(GIS, query, [], callback);
}


var storeNames = function(callback){
    for (var i in callback.rows) {
        var obj = callback.rows[i];
        if (obj.name) {

            var _nameArray = obj.name.split(" ");

            counter += _nameArray.length;

            _nameArray.forEach(function (name) {
                var cb = {};

                cb.list_next = callback.list;
                cb.osmId = obj.osm_id;
                cb.type = current;
                cb.name = name;
                cb.end = afterCreate;
                namesController.storeName(cb);
            });
        }
    }
}

/**
 * Internal use: it updates the 'type_id' column in the tables of the temporary database according to the
 * values of the attributes of the record
 * @param callback Callback object in which are stored the properties list
 */
var applyProperties = function(callback){

    /*
    validation for the list of properties stored within the callback object
     */
    val.assertNotUndefined(callback.types, 'oi03');


    /*
    the appropriate type identifier is applied to each record
     */
    callback.types.forEach(function (entry){
        if (entry.type_id >1) {
            var query = queries.applyTypeId(callback, entry.type_key);
            var params = [entry.type_id, entry.type_value];
            database.execute(TEMP, query, params, undefined);
        }
    });

    cbu.next(callback,"error 003");
}

/**
 * Internal use: it copies the records from the temporary table to the main table
 * @param callback Callback object
 */
var moveRows = function(callback){
    database.execute(GIS, queries.moveRows(callback), [], callback);
}

/**
 * Internal use: it set the timestamp of the operation as a value for the column 'modified_on'
 * @param callback Callback object
 */
var setTimestamp = function(callback){
    database.execute(GIS, queries.setTimestamp(callback), [new Date().toISOString()], callback);
}

/**
 * Internal use: it deletes the old records stored in the main database. The process is based on the
 * soft delete approach.
 * @param callback Callback object in which is stored the list of identifier
 */
var deleteRecords = function(callback) {
    /*
    validation of the query execution result, used to ensure the existence of record to store into the main database
     */
    val.assertRows(callback);

    /*
    the soft delete process is applied for each identifier stored in the temporary database
     */
    callback.rows.forEach(function (entry) {

        /*
        assert that the identifier is valid
         */
        val.assertNotUndefined(entry, 'oi04');

        /*
        query execution
         */
        database.execute(GIS, queries.softDelete(callback), [entry.osm_id], {});
    });

    cbu.next(callback,"error 002");
}

/**
 * Internal use: it retrieves a list of properties for the given entity, in order to assign the correct type
 * identifier to each record before to store them in the main database.
 * @param callback Callback object in which is stored the entity type used to select the correct table
 */
var getProperties = function(callback){

    /*
    query execution
     */
    database.execute(GIS, queries.getProperties(callback), [], callback);
}

/**
 * Internal use: it manages the result of the query executed by the 'getProperties' method. It is stored
 * the callback object
 * @param callback Callback object in which is stored the result of the query execution
 */
var getProperties_aux = function(callback){

    /*
    validation of the query execution result
     */
    val.assertRows(callback);

    /*
    the result is stored in the callback object
     */
    callback.types = [];

    /*
    copy process
     */
    callback.rows.forEach(function(entry){
        callback.types.push(entry);
    });

    cbu.next(callback,"error 007");
}

/**
 * Internal use: it adds some columns to the temporary table of the current entity
 * @param callback Callback object
 */
var setTable = function(callback){
    database.execute(GIS, queries.setTable(callback), [], callback);
}

/**
 * Internal use: this method is called after the creation process, and it updates the counter by decrementing it.
 * When the count reaches the zero, the workflow for the current entity ends.
 */
var afterCreate = function(callback){

    val.assertNotUndefined(callback, "oi099");

    /*
    check the counter validity
     */
    if (counter >= 0){

        /*
        at this point the creation of a new record within the main database is ensure, then the counter value
        is decremented
         */
        counter --;

        /*
        when the counter reaches  zero, then workflow for the current entity ends.
         */
        if (counter <= 0){
            callback.list = callback.list_next;
            cbu.next(callback,"error 009");
        }
    }
}

/**
 * Internal use: this method is called when the workflow for an entity ends. It calls the init method
 */
var endEntity = function(){
    init();
}

/**
 * Internal use: it initializes the callback object and the workflow for the selected entity. Then the
 * process starts with the first call
 */
var startProcess = function(){

    /**
     * Callback object init
     * @type {{params: {type: number}, list: Array}} It contains the identified of the current entity type
     * and the empty initialization of the list field used as a queue of method calls
     */
    var callback = {
        params : {
            type : current
        },
        list : []
    };

    /*
    debug message
     */
    log.debug("CURRENT: "+params.typeDescriptions[current]);

    /*
    workflow
     */
    callback.list.push(endEntity);
    callback.list.push(moveRows);
    callback.list.push(deleteRecords);
    callback.list.push(storeNames);
    callback.list.push(getNames);
    callback.list.push(setTimestamp);
    callback.list.push(applyProperties);
    callback.list.push(getProperties_aux);
    callback.list.push(getProperties);


    setTable(callback);
}

/**
 * It manages the import process
 */
var init = function(){

    /*
    the counter variable, which represent the number of record to insert/update and used for
    progression purposes is reset
     */
    counter = 0;

    /*
    if the current entity identifier is undefined, it means that it is the first execution.
     */
    if (current === undefined){

        /*
        if it is the first execution, then then the first entity is selected
         */
        current = 0;
    } else {

        /*
        if it is not the first execution, then the next entity is selected
         */
        current++;
    }

    /*
    check if the identifier of the current entity it is within the range
     */
    if (current < params.types.length){
        /*
        start the import process for the selected entity
         */
        startProcess();
    } else {
        /*
        otherwise the process has been completed for all the entities. At this point the program ends.
         */
        log.end();
    }
}

module.exports.init = init;