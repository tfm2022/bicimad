db.Meteo_US_NivelHora.find({})
   .projection({})
   .sort({_id:-1})
   .limit(100)

db.Meteo_US_NivelHora.drop()

db.getCollection('Meteo_US_Original')
    .aggregate([
        { $out: 'Meteo_US_NivelHora' }
    ]);


db.Meteo_US_NivelHora.updateMany({}, {$unset: {dt: 1, timezone: 1, city_name: 1, visibility: 1, lat:1, lon:1, 
dew_point:1, temp_min:1, temp_max:1, wind_deg:1, wind_gust:1, clouds_all:1, weather_icon:1, sea_level:1, grnd_level:1}})


db.Meteo_US_NivelHora.updateMany({'rain_1h': {$exists: false}}, {$set: {'rain_1h': 0}})
db.Meteo_US_NivelHora.updateMany({'rain_3h': {$exists: false}}, {$set: {'rain_3h': 0}})
db.Meteo_US_NivelHora.updateMany({'snow_1h': {$exists: false}}, {$set: {'snow_1h': 0}})
db.Meteo_US_NivelHora.updateMany({'snow_3h': {$exists: false}}, {$set: {'snow_3h': 0}})

/*
db.Meteo_US_NivelHora.updateMany(
    {}, 
    [
        {'$set': {'ANIO': {$year: { $toDateTime: "$dt_iso" }}}},
        {'$set': {'MES': {$month: { $toDate: "$dt_iso" }}}},
        {'$set': {'DIA': {$dayOfMonth: { $toDate: "$dt_iso" }}}}
    ])
    */

db.Meteo_US_NivelHora.updateMany(
    {}, 
    [
        {'$set': 
            {'ANIO': {$toInt: {$substr: [ "$dt_iso", 0, 4 ]}},
       'MES': {$toInt: {$substr: [ "$dt_iso", 5, 2 ]}},
       'DIA': {$toInt: {$substr: [ "$dt_iso", 8, 2 ]}},
       'HORA': {$toInt: {$substr: [ "$dt_iso", 11, 2 ]}}}
        }
    ]
)

