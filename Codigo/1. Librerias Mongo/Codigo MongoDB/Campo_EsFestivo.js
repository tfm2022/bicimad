//13.684.833

/*
db.Tracks_Demanda.updateMany(
    {'ANIO': 2018,'MES':1},{$set:{'Es_Festivo':0}})
*/

db.Tracks_Demanda.aggregate([
    {'$match': {'ANIO': 2018, 'MES':1}},
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
    
    
db.Tracks_Demanda.updateMany({'ANIO': 2018,'MES':1, 'DIA':11},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2018,'MES':1, 'DIA':19},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2018,'MES':1, 'DIA':26},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2018,'MES':1, 'DIA':28},{$set:{'Es_Festivo':0}})
    
db.Tracks_Demanda.updateMany({'ANIO': 2019,'MES':1, 'DIA':11},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2019,'MES':1, 'DIA':19},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2019,'MES':1, 'DIA':26},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2019,'MES':1, 'DIA':29},{$set:{'Es_Festivo':0}})
    
db.Tracks_Demanda.updateMany({'ANIO': 2020,'MES':1, 'DIA':12},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2020,'MES':1, 'DIA':19},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2020,'MES':1, 'DIA':27},{$set:{'Es_Festivo':0}})
db.Tracks_Demanda.updateMany({'ANIO': 2020,'MES':1, 'DIA':28},{$set:{'Es_Festivo':0}})
    
    
/*
db.Tracks_Demanda.aggregate([
    {
        $match: {'ANIO':2018, 'MES':1}
    },
    {
        $group: { 
            _id: 
            {
                'ANIO': '$ANIO',
                'MES': '$MES',
                'DIA': '$DIA',
                'ANIOMESDIA': '$ANIOMESDIA',
                'Es_Festivo': '$Es_Festivo'
            },
            'cuenta': {$sum: 1}
        }
    },
    {
        '$sort': {'_id':1}
    }
    ])
    */