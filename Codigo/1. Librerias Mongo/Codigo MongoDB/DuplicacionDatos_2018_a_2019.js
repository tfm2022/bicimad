/*** CREACION DE COLLECTION DE TRACKS PARA JULIO/18 ***/
db.Tracks_Julio18.drop()

db.Tracks.aggregate([
    {
      $match: {$and: [{'ANIO': {$eq: 2018}}, {'MES': {$eq: 7}}]}  
    }]).allowDiskUse().out("Tracks_Julio18")
    
db.Tracks_Julio18.updateMany(
    {},
    {$set: {'ANIO': 2019}})
    
db.Tracks_Julio18.deleteMany({'DIA': {$lte: 11}}) // Se borran todos los dias antes del 12
db.Tracks_Julio18.find()


/*** CREACION DE COLLECTION DE TRACKS_DEMANDA PARA JULIO/18 ***/
db.Tracks_Demanda.aggregate([
    {
      $match: {$and: [{'ANIO': {$eq: 2018}}, {'MES': {$eq: 7}}]}  
    }]).allowDiskUse().out("Tracks_Demanda_Julio18")
    
db.Tracks_Demanda_Julio18.updateMany(
    {},
    {$set: {'ANIO': 2019}})
    
db.Tracks_Demanda_Julio18.deleteMany({'DIA': {$lte: 11}}) // Se borran todos los dias antes del 12
db.Tracks_Demanda_Julio18.find()



/***********************************************
 * COPIA DE DATOS HASTA TRACKS y TRACKS_DEMANDA */
 
/* TRACKS */

db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':12})
db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':15})
db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':16})
db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':26})
db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':28})
db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':29})
db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':30})
db.Tracks.deleteMany({'ANIO':2019, 'MES':7,'DIA':31})

db.Tracks_Julio18.aggregate([
    {$match:{'ANIO':2019, 'MES':7}},
    {$group:{
            _id: '$DIA',
            count: {$sum: 1}}},
    {$sort:{_id:1}}])

// Proceso de copia
var i=1;
db.Tracks_Julio18.find({'ANIO':2019, 'MES':7}).forEach(function(doc){
    print(i);
    i++;
    doc._id = new ObjectId();
    db.Tracks.insert(doc)
});


db.Tracks.aggregate([
    {$match:{'ANIO':2019, 'MES':7}},
    {$group:{
            _id: '$DIA',
            count: {$sum: 1}}},
    {$sort:{_id:1}}])
    

/* TRACKS_DEMANDA */

db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':12})
db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':15})
db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':16})
db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':26})
db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':28})
db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':29})
db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':30})
db.Tracks_Demanda.deleteMany({'ANIO':2019, 'MES':7,'DIA':31})

db.Tracks_Demanda_Julio18.aggregate([
    {$match:{'ANIO':2019, 'MES':7}},
    {$group:{
            _id: '$DIA',
            count: {$sum: 1}}},
    {$sort:{_id:1}}])

// Proceso de copia
var i=1;
db.Tracks_Demanda_Julio18.find({'ANIO':2019, 'MES':7}).forEach(function(doc){
    print(i);
    i++;
    doc._id = new ObjectId();
    db.Tracks_Demanda.insert(doc)
});


db.Tracks_Demanda.aggregate([
    {$match:{'ANIO':2019, 'MES':7}},
    {$group:{
            _id: '$DIA',
            count: {$sum: 1}}},
    {$sort:{_id:1}}])



/*** MODIFICACION DE VALORES PARA VARIABLES unplut_hourTime, DIA_SEMANA, Es_Festivo y Es_FinSemana ***/

/** UNPLUG_TIME_date **/
db.Tracks_Demanda.updateMany( //FECHAS DE 2018 AHORA DEBEN SER 2019
    {'ANIO':2019, 'MES':7},
    [{'$set':{'UNPLUG_TIME_date':{$dateFromString:{dateString: {$concat: ['2019-07-', {'$toString': '$DIA'},' ', {'$toString': '$HORA'},':00:00']}}}}}])
    
/** DIA_SEMANA */
db.Tracks_Demanda.updateMany(
    {'ANIO':2019, 'MES':7},
    [{'$set': {'DIA_SEMANA': {$dayOfWeek: '$UNPLUG_TIME_date'}}}])

/** Es_FinSemana **/
db.Tracks_Demanda.updateMany(
    {'ANIO':2019, 'MES':7},
    [{'$set': {'Es_FinSemana': 0}}])
    
db.Tracks_Demanda.updateMany(
    {'ANIO':2019, 'MES':7, 'DIA_SEMANA':1}, 
    [{
        '$set': {'Es_FinSemana':1}
    }])
    
db.Tracks_Demanda.updateMany(
    {'ANIO':2019, 'MES':7, 'DIA_SEMANA':7}, 
    [{
        '$set': {'Es_FinSemana':1}
    }])

/** Es_Festivo **/
db.Tracks_Demanda.updateMany(
    {'ANIO':2019, 'MES':7}, 
    [{
        '$set': {'ANIOMESDIA':{$concat: [{'$toString': '$ANIO'}, {'$toString': '$MES'}, {'$toString': '$DIA'}]}}
    }])


db.Tracks_Demanda.aggregate([
    {'$match': {'ANIO': 2019, 'MES':7}},
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