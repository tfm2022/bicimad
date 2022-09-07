db.Tracks.drop()
db.Tracks_Demanda.drop()

db.getCollection('Tracks_Original')
    .aggregate([
        { $out: 'Tracks' }
    ]);


//db.Tracks_Original.count()
//db.Tracks.count()

//db.Tracks_Original.dataSize()
//db.Tracks.dataSize()


db.Tracks.updateMany({}, {$unset: {user_day_code: 1, idplug_base: 1, idunplug_base: 1, track: 1, idplug_station:1}})

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
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 3]}, then: "INVIERNO" },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 6]}, then: "PRIMAVERA" },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 9]}, then: "VERANO" },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 12]}, then: "OTONO" }
                    ]
                }}}},
        {'$set': {'TEMPORADA_NUM': { $switch: {
                    branches: [
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 3]}, then: 1 },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 6]}, then: 2 },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 9]}, then: 3 },
                        { case: {$lte: [{$month: '$UNPLUG_TIME_date'}, 12]}, then: 4 }
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
     
     
// COLECCION AGRUPADOS JUNTO CON CAMPO DE DEMANDA
fase1 = {$set: {'ANIOMESDIA': {
                $concat: [{'$toString': '$ANIO'}, {'$toString': '$MES'}, {'$toString': '$DIA'}]}}}
fase2 = {
      $group: {
         _id : {'$concat': [
             {'$toString': '$ANIO'}, {'$toString': '$MES'}, {'$toString': '$DIA'}, {'$toString': '$HORA'},
             {'$toString': "$idunplug_station"},
             {'$toString': "$travel_time"},
             {'$toString': "$user_type"},
             {'$toString': "$ageRange"},
             {'$toString': "$zip_code"}]},
        'ANIO': {$first: '$ANIO'},
        'MES': {$first: '$MES'},
        'DIA': {$first: '$DIA'},
        'HORA': {$first: '$HORA'},
        'DIA_SEMANA': {$first: '$DIA_SEMANA'},
        'AM_PM': {$first: '$AM_PM'},
        'TEMPORADA': {$first: '$TEMPORADA'},
        'TEMPORADA_NUM': {$first: '$TEMPORADA_NUM'},
        'UNPLUG_TIME_date': {$first: '$UNPLUG_TIME_date'},
        'travel_time': {$first: '$travel_time'},
        'idunplug_station': {$first: '$idunplug_station'},
        'user_type': {$first: '$user_type'},
        'ageRange': {$first: '$ageRange'},
        'zip_code': {$first: '$zip_code'},
        'ANIOMESDIA': {$first: '$ANIOMESDIA'},
        'DEMANDA' : { $sum: 1 }
      }
    }
    
db.Tracks.aggregate([fase1,fase2]).allowDiskUse().out("Tracks_Demanda")

/*db.Tracks_Demanda.updateMany(
    {}, 
    [{
        '$set': {
            'Es_FinSemana': { 
                $switch: {
                    branches: [
                        { case: {'DIA_SEMANA': 1}, then: 1 },    //DOMINGO
                        { case: {'DIA_SEMANA': 7}, then: 1 }],    //SABADO
                        default: 0
                }
            }
        }
    }])*/
    
db.Tracks_Demanda.updateMany(
    {}, 
    [{
        '$set': {'Es_FinSemana': 0}
    }])
    
db.Tracks_Demanda.updateMany(
    {'DIA_SEMANA':1}, 
    [{
        '$set': {'Es_FinSemana':1}
    }])
    
db.Tracks_Demanda.updateMany(
    {'DIA_SEMANA':7}, 
    [{
        '$set': {'Es_FinSemana':1}
    }])

db.Tracks_Demanda.createIndex(
                  {"ANIO": 1, "MES":1},  //MongoDB 4.2 wildcard index on a specific field { "field.$**" : 1 }
                  {
                    background:true,//Specify true to build in the background.
                    unique:false, //Specify true to create a unique index. A unique index causes MongoDB to reject all documents that contain a duplicate value for the indexed field.
                    name: "idxDemanda_AnioMes",   //The name of the index.     
                    //partialFilterExpression: { field: { $exists: true } }, // If specified, the index only references documents that match the filter expression
                    //sparse:false, //If true, the index only references documents with the specified field. Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.  partial indexes should be preferred over sparse indexes.

                    //expireAfterSeconds:0, //Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
                    //storageEngine:{WiredTiger: <options> }
                    //collation: { locale: "zh", strength: 2 } //Specifies the collation for the index.
                 }
     )


db.Tracks_Demanda.createIndex(
                  {"ANIO": 1},  //MongoDB 4.2 wildcard index on a specific field { "field.$**" : 1 }
                  {
                    background:true,//Specify true to build in the background.
                    unique:false, //Specify true to create a unique index. A unique index causes MongoDB to reject all documents that contain a duplicate value for the indexed field.
                    name: "idxDemanda_Anio",   //The name of the index.     
                    //partialFilterExpression: { field: { $exists: true } }, // If specified, the index only references documents that match the filter expression
                    //sparse:false, //If true, the index only references documents with the specified field. Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.  partial indexes should be preferred over sparse indexes.

                    //expireAfterSeconds:0, //Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
                    //storageEngine:{WiredTiger: <options> }
                    //collation: { locale: "zh", strength: 2 } //Specifies the collation for the index.
                 }
     )


db.Tracks_Demanda.createIndex(
                  {"idunplug_station": 1},  //MongoDB 4.2 wildcard index on a specific field { "field.$**" : 1 }
                  {
                    background:true,//Specify true to build in the background.
                    unique:false, //Specify true to create a unique index. A unique index causes MongoDB to reject all documents that contain a duplicate value for the indexed field.
                    name: "idxDemanda_IdUnplugStation",   //The name of the index.     
                    //partialFilterExpression: { field: { $exists: true } }, // If specified, the index only references documents that match the filter expression
                    //sparse:false, //If true, the index only references documents with the specified field. Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.  partial indexes should be preferred over sparse indexes.

                    //expireAfterSeconds:0, //Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
                    //storageEngine:{WiredTiger: <options> }
                    //collation: { locale: "zh", strength: 2 } //Specifies the collation for the index.
                 }
     )
     
db.getCollection("Tracks_Demanda").createIndex({ "Es_FinSemana": 1 }, {
    "name": "Idx_EsFinSemana"
})
     
/*
db.Festivos_Madrid.updateMany(
    {}, 
    [
        {'$set': {'ANIOMESDIA': {
                $concat: [{'$toString': '$ANIO'}, {'$toString': '$MES'}, {'$toString': '$DIA'}]}}}
    ])*/
    
    
// Se agrega el campo Es_Festivo en base a un lookup realizado contra la colección Festivos_Madrid
db.Tracks_Demanda.aggregate([
    //{'$match': {'ANIO': 2017, 'MES':4}},
    {
        '$lookup': {
               'from': "Festivos_Madrid",
               'localField': 'ANIOMESDIA',
               'foreignField': 'ANIOMESDIA',
               'as': "Es_Festivo"
             }
    }, 
    {
      '$unwind': {'path': '$Es_Festivo'}  
    },
    {
        '$addFields': {'Es_Festivo': {'$toInt': '$Es_Festivo.FESTIVO'}}
    },
    {
        $merge: {
            into: 'Tracks_Demanda',
            whenMatched: 'replace',
            whenNotMatched: 'discard'
        }
    }
    ])