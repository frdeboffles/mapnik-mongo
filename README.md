# MongoDB input plugin for Mapnik

This is a connector to MongoDB data in the form of a Mapnik C++ plugin.
MongoDB supports spatial indexing on a sphere for a Points, a LineStrings and a 
Polygons since version 2.4.

# Usage

Input plugin accepts the following parameters:
 * uri -- (optional) uri to connect MongoDB [default: "mongodb://localhost:27017"]
 * database -- (optional) database name to use [default: "gis"]
 * collection -- (optional) collection to use [default: "points"]
 * geometry -- (optional) key to the geometry document in the mongodb results to 
 use [default: "geometry"]
 * geometry_index_2d -- (optional) set to true if you want to use the `2d` index on 
 the `<geometry>.coordinates` [default: false]
 * filter -- (optional) mongodb filter in json format to use [default: "{}"]
 * log_level -- (optional) set the plugin log level. Values can be `debug`, `warn`, `error`, `none`
  [default: "none"]

Example in XML:

    <Datasource>
        <Parameter name="type">mongodb</Parameter>
        <Parameter name="uri">mongodb://localhost:27017/?minPoolSize=2&maxPoolSize=6</Parameter>
        <Parameter name="collection">polygons</Parameter>
    </Datasource>
    
Records in the database should have a "geometry" property with GeoJSON geometry (only Point, 
LineString and Polygon are supported), and a "properties" property, which contains an information 
about feature.

Example:

    {
        geometry: {
            type: "LineString",
            coordinates: [ [ lng1, lat1 ], [ lng2, lat2 ], ... ]
        },
        name: "Long Hard Road",
        id: 32167,
        ...
    }
    
CAUTION: notice the Longitude, Latitude order.

# Run the test example using docker

    docker-compose build node-mongo-mapnik-test
    docker-compose run node-mongo-mapnik-test
    
This will create a sample mongo database with the features from the `points`, `linestrings`
and `polygons` shapefiles and then render 2 tiles using this plugin.

You can then find the generated pngs in the `./test-res` folder

