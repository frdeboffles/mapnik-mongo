'use strict';

var path = require('path');
var mapnik = require('mapnik'), mongodb = require('mongodb');

mapnik.register_datasources('/opt/mapnik/lib/mapnik/input');
var nReady = 0;
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/gis';

[ 'points', 'linestrings', 'polygons' ].forEach(function(name) {
    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        var dbo = db.db('gis');
        dbo.createCollection(name, function (err, collection) {
            if (err) throw err;
            console.log('Collection ' + name + ' created');
            collection.createIndex({ geometry: '2dsphere' }, function(err) {
                if (err) throw err;
                console.log('2dsphere index on ' + name + ' created');
            });
            importShp(name, collection, db);
        });
    });
});

function importShp(name, collection, db) {
    var dataSource = new mapnik.Datasource({
            type: 'shape',
            file: path.join('shp', name)
        });
    var featureSet = dataSource.featureset();

    function next(feature) {
        if (!feature) {
            if (++nReady === 3) { // stupidly async chains counting :-)
                console.log('done...');
                process.exit();
            }
            db.close();
            return;
        }

        var json = JSON.parse(feature.toJSON());
        console.log('insert feature in ' + name, json);
        collection.insertOne(json, function(err) {
            if (err) {
                console.log('inserting error:', err.message);
            }
            next(featureSet.next());
        });
    }
    next(featureSet.next());
}


