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

/**
 * Internal use: it retrieves a record from the temporary database
 * @param callback Callback object in which are stored the entity type and the record identifier
 */
var getRecord = function(callback){
    /*
    entity type validation, used to select the correct table
     */
    val.assertType(callback);

    /*
    identifier validation, used to retrieve the correct record(s)
     */
    val.assertId(callback);

    /*
    the call for the record insertion (into the main database) is added to the process flow
     */
    callback.list.push(createRecord);

    /*
    query execution
     */
    database.execute(TEMP, queries.getById(callback), [callback.params.id], callback);
}

/**
 * Internal use: it stores a new record (or records, since the osm_id attribute is not a primary key
 * due to the fact that the tool osm2pgsql split some geometries) into the main database
 * @param callback Callback object in which are stored information about the records to insert
 */
var createRecord = function(callback){
    /*
    entity type validation, used to select the correct table
     */
    val.assertType(callback);

    /*
    validation of the query execution result, used to ensure the existence of record to store into the main database
     */
    val.assertRows(callback);

    /*
    as described before, there can be more than one record for a given identifier. Then the result of the
    query execution could contains more than one row
     */
    for (var k in callback.rows) {

        /**
         * Short link for the current record
         */
        var obj = callback.rows[k];

        /*
        if the record is set as deleted, then it is skipped
         */
        if (!obj.is_deleted){

            /**
             * Declaration and initialization of the string with the list of fields used
             * in within the insertion query.
             * @type {string} String which represents the list of fields
             */
            var query_fields = "";

            /**
             * Declaration and initialization of the string with the list of values used
             * in within the insertion query.
             * @type {string} String which represents the list of values
             */
            var query_values = "";

            /**
             * A boolean flag used to determine the first entry of the the list of fields. It is initialized as true
             * and the value is updated only at the first step of the loop
             * @type {boolean} true if the current field is the first one that has been selected, false otherwise
             */
            var firstField = true;

            /**
             * A boolean flag used to determine the first entry of the the list of values. It is initialized as true
             * and the value is updated only at the first step of the loop
             * @type {boolean} true if the current values is the first one that has been selected, false otherwise
             */
            var firstValue = true;

            /**
             * An array used to store the values of the current object. It is initialized as empty
             * @type {Array} Array which contains the values of the attributes of the current object
             */
            var values = [];

            /**
             * The counter of the fields used to associate the values in the parametrized query.
             * It is initialized as 1 (one).
             * @type {number} An integer which represent the field examined during the construction of the
             * insertion query
             */
            var fieldCounter = 1;

            /*
            The record is updated with the date time of insertion process, in order to maintain
            an history of the inserted or updated records
             */
            obj.modified_on = params.timestamp();

            /*
            iteration of the fields of the record
             */
            for (var i in obj) {

                /*
                by construction of the tables, some columns have null as value. This fields are skipped
                 */
                if (obj[i] !== null) {

                    /*
                    since the way column contains the geometry, it is directly embedded in the query
                     */
                    if (i !== 'way') {
                        values.push(obj[i]);
                    }

                    /*
                    fields are separated with a comma, except the first entry
                     */
                    if (firstField) {
                        firstField = false;
                    } else {
                        query_fields += ", ";
                    }

                    /*
                    field list construction
                     */
                    query_fields += "\"" + i + "\"";

                    /*
                    values are separated with a comma, except the first entry
                     */
                    if (firstValue) {
                        firstValue = false;
                    } else {
                        query_values += ", ";
                    }

                    /*
                    value list construction
                     */
                    if ((i === 'z_order') || (i === 'type_id')) {
                        query_values += "$" + fieldCounter++ + "::integer";
                    } else if (i === 'way_area') {
                        query_values += "$" + fieldCounter++ + "::float";
                    } else if (i === 'modified_on') {
                        query_values += "$" + fieldCounter++ + "::timestamp";
                    } else if (i === 'osm_id') {
                        query_values += "$" + fieldCounter++ + "::bigint";
                    } else if (i === 'way') {
                        query_values += "'" + obj[i] + "'";
                    } else {
                        query_values += "$" + fieldCounter++ + "::varchar";
                    }
                }
            }

            /*
            query execution. The call to the 'afterCreate' method is added to the process flow for
            progression purposes
             */
            database.execute(GIS, queries.insert(callback, query_fields, query_values), values, {list: [afterCreate]});

//            console.log(callback.params.type);

            namesController.storeNames(obj, callback.params.type);
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
    val.assertNotUndefined(callback.types);


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
        val.assertNotUndefined(entry);

        /*
        query execution
         */
        database.execute(GIS, queries.softDelete(callback), [entry.osm_id], {});
    });

    cbu.next(callback,"error 002");
}

/**
 * Internal use: it retrieves a list of identifier of the records to insert from the temporary database.
 * The entity type (used to select the correct table) is saved in the callback object.
 * @param callback Callback object in which is stored the entity type
 */
var getIds = function(callback){

    /*
    query execution
     */
    database.execute(TEMP, queries.getIds(callback), [], callback);
}

/**
 * Internal use: it retrieves the number of record to insert/update in the main database
 * @param callback Callback object in which is stored the entity type
 */
var countEntries = function(callback){

    /*
     query execution
     */
    database.execute(TEMP, queries.countEntries(callback), [], callback);
}

/**
 * Internal use: it manages the result of the query executed by the 'countEntries' method, saving it
 * in the 'counter' variable used for progression purposes during the process flow
 * @param callback Callback object in which is stored the result of the query execution
 */
var countEntries_aux = function(callback){
    /*
    validation of the query execution result
     */
    val.assertRows(callback);


    /*
    assert the validity of the first entry of the list
     */
    val.assertNotUndefined(callback.rows[0]);

    /*
    assert the validity of an attribute of the first entry of the list
     */
    val.assertNotUndefined(callback.rows[0]['osm_id']);


    /*
    at this point the validity of the value which represents the number of record to store is ensure.
    Then the value is stored into a global variable and used for progression purposes.
     */
    counter = callback.rows[0]['osm_id'];

    /*
    debug message
     */
    log.debug("It has been detected "+counter+ " rows");

    cbu.next(callback,"error 005");
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
 * Internal use: for each record, it retrieves it from the temporary database and it store it into the main database.
 * @param callback Callback object in which is stored the list of identifier used to select and store the records
 */
var store = function(callback){

    /*
    debug message
     */
    log.debug("store");

    /*
     validation of the query execution result
     */
    val.assertRows(callback);

    /*
    for each record a new callback object is instantiated. Then the query to retrieve the record from the
    temporary database is executed
     */
    callback.rows.forEach(function(entry){

        var _callback = {
            params : {
                id : entry.osm_id,
                type : callback.params.type
            },
            list : []
        };
        getRecord(_callback);
    });
}

/**
 * Internal use: this method is called after the creation process, and it updates the counter by decrementing it.
 * When the count reaches the zero, the workflow for the current entity ends.
 */
var afterCreate = function(){
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
        when the counter reaches the zero, then workflow for the current entity ends.
         */
        if (counter === 0){
            endEntity();
        }
    } else {

        /*
        error, the counter is invalid
         */
        log.error("invalid counter");
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
    callback.list.push(store);
    callback.list.push(deleteRecords);
    callback.list.push(getIds);
    callback.list.push(countEntries_aux);
    callback.list.push(countEntries);
    callback.list.push(applyProperties);
    callback.list.push(getProperties_aux);

    getProperties(callback);
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
    if (current <= params.POINT){

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