/**
 * @author Trilogis Srl
 * @author Gustavo German Soria
 *
 * Clean queries module
 */

/**
 * Query used to erase the temporary database
 * @returns {string} the query string
 */
var clean = function(){
    return "    DELETE FROM planet_osm_line;" +
               "DELETE FROM planet_osm_roads;" +
               "DELETE FROM planet_osm_polygon;" +
               "DELETE FROM planet_osm_point;" ;
}

module.exports.clean = clean;