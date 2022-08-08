db.Tracks.drop()

db.getCollection('Tracks_Original')
    .aggregate([
        { $out: 'Tracks' }
    ]);


db.Tracks_Original.count()
db.Tracks.count()

db.Tracks_Original.dataSize()
db.Tracks.dataSize()


db.Tracks.updateMany({}, {$unset: {user_day_code: 1, idplug_base: 1, idunplug_base: 1, track: 1}})

db.Tracks.updateMany(
    {}, 
    [
        {'$set': 
            {'UNPLUG_TIME_date': { $toDate: "$unplug_hourTime" }}
        }
    ]
)

db.Tracks.updateMany(
    {}, 
    [
        {'$set': {'ANIO': {$year: '$UNPLUG_TIME_date'}}},
        {'$set': {'MES': {$month: '$UNPLUG_TIME_date'}}},
        {'$set': {'DIA': {$dayOfMonth: '$UNPLUG_TIME_date'}}},
        {'$set': {'DIA_SEMANA': {$dayOfWeek: '$UNPLUG_TIME_date'}}},
        {'$set': {'HORA': {$hour: '$UNPLUG_TIME_date'}}},
        {'$set': {'TEMPORADA': { $switch: {
                    branches: [
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 3]}, then: "Invierno" },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 6]}, then: "Primavera" },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 9]}, then: "Verano" },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 12]}, then: "Otoño" }
                    ]
                }}}},
        {'$set': {'AM_PM': {$cond: { if: { $lt: [{$hour: '$UNPLUG_TIME_date'}, 12] }, then: 'AM', else: 'PM'}}}}
    ]
)

db.Tracks.createIndex(
                  {"ANIO": 1, "MES":1},  //MongoDB 4.2 wildcard index on a specific field { "field.$**" : 1 }
                  {
                    background:true,//Specify true to build in the background.
                    unique:false, //Specify true to create a unique index. A unique index causes MongoDB to reject all documents that contain a duplicate value for the indexed field.
                    name: "idx_AnioMes",   //The name of the index.     
                    //partialFilterExpression: { field: { $exists: true } }, // If specified, the index only references documents that match the filter expression
                    //sparse:false, //If true, the index only references documents with the specified field. Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.  partial indexes should be preferred over sparse indexes.

                    //expireAfterSeconds:0, //Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
                    //storageEngine:{WiredTiger: <options> }
                    //collation: { locale: "zh", strength: 2 } //Specifies the collation for the index.
                 }
     )


db.Tracks.createIndex(
                  {"ANIO": 1},  //MongoDB 4.2 wildcard index on a specific field { "field.$**" : 1 }
                  {
                    background:true,//Specify true to build in the background.
                    unique:false, //Specify true to create a unique index. A unique index causes MongoDB to reject all documents that contain a duplicate value for the indexed field.
                    name: "idx_Anio",   //The name of the index.     
                    //partialFilterExpression: { field: { $exists: true } }, // If specified, the index only references documents that match the filter expression
                    //sparse:false, //If true, the index only references documents with the specified field. Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.  partial indexes should be preferred over sparse indexes.

                    //expireAfterSeconds:0, //Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
                    //storageEngine:{WiredTiger: <options> }
                    //collation: { locale: "zh", strength: 2 } //Specifies the collation for the index.
                 }
     )


db.Tracks.createIndex(
                  {"idunplug_station": 1},  //MongoDB 4.2 wildcard index on a specific field { "field.$**" : 1 }
                  {
                    background:true,//Specify true to build in the background.
                    unique:false, //Specify true to create a unique index. A unique index causes MongoDB to reject all documents that contain a duplicate value for the indexed field.
                    name: "idx_IdUnplugStation",   //The name of the index.     
                    //partialFilterExpression: { field: { $exists: true } }, // If specified, the index only references documents that match the filter expression
                    //sparse:false, //If true, the index only references documents with the specified field. Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.  partial indexes should be preferred over sparse indexes.

                    //expireAfterSeconds:0, //Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
                    //storageEngine:{WiredTiger: <options> }
                    //collation: { locale: "zh", strength: 2 } //Specifies the collation for the index.
                 }
     )