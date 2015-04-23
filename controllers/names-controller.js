/**
 * @author Trilogis Srl
 * @author Gustavo German Soria
 *
 * Names Controller
 */

/**
 * Import of the searchDAO module
 * @type {exports}
 */
var searchDao = require('./../dao/search-dao.js');

/**
 * Import of the validation module
 * @type {exports}
 */
var val = require('./../validation/validation.js');


/**
 * It stores the name values for the given entity, by splitting them in single words. For each word,
 * if the insertion is successful, then the identifier of the new record is returned. Otherwise the
 * insertion fails, and the identifier of the existing record is returned.
 *
 * @param record The object to store
 * @param type the entity identifier
 */
var storeNames = function(record, type){

    /*
     the store process begins only if the record has the name attribute
     */
    if (record.name){

        /*
         split process
         */
        var _nameArray = record.name.split(" ");

        /*
         each partial name value is stored
         */
        _nameArray.forEach(function(name){

            /*
             callback object construction
             */
            var _callback = {
                osmId   : record.osm_id,
                value   : name,
                params  : {type: type},
                list    : [storePartialName],
                onError : selectPartialName
            };

            /*
             store
             */
            searchDao.storePartialName(_callback);
        });
    }
}

/**
 * Internal use: it retrieves the identifiers of the partial name values
 * @param callback Callback object
 */
var selectPartialName = function(callback) {

    /*
    execute request
     */
    searchDao.selectPartialName(callback);
}


/**
 * Internal use: it stores the name value into the relationship table
 * @param callback
 */
var storePartialName = function(callback) {

    /*
     it validates the result of the query execution
     */
    val.assertRows(callback);

    /*
     it ensure that the first entry of the query execution result is valid
     */
    val.assertNotUndefined(callback.rows[0]);

    /*
     it ensure that the field name_id of the first entry of the query execution result is valid
     */
    val.assertNotUndefined(callback.rows[0].name_id);

    /*
    the value is stored in the callback object
     */
    callback.nameId = callback.rows[0].name_id;

    /*
    store process
     */
    searchDao.storeNameRelation(callback);
}

module.exports.storeNames = storeNames;