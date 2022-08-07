db.Meteo_Original.createIndex(
                  {"ANO": 1, "MES": 1},  //MongoDB 4.2 wildcard index on a specific field { "field.$**" : 1 }
                  {
                    background:true,//Specify true to build in the background.
                    unique:false, //Specify true to create a unique index. A unique index causes MongoDB to reject all documents that contain a duplicate value for the indexed field.
                    name: "idx_AnoMes",   //The name of the index.     
                    //partialFilterExpression: { field: { $exists: true } }, // If specified, the index only references documents that match the filter expression
                    //sparse:false, //If true, the index only references documents with the specified field. Starting in MongoDB 3.2, MongoDB provides the option to create partial indexes.  partial indexes should be preferred over sparse indexes.

                    //expireAfterSeconds:0, //Specifies a value, in seconds, as a TTL to control how long MongoDB retains documents in this collection.
                    //storageEngine:{WiredTiger: <options> }
                    //collation: { locale: "zh", strength: 2 } //Specifies the collation for the index.
                 }
     )
     
