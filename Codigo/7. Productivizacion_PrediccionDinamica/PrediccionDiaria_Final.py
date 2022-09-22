#!/usr/bin/env python
# coding: utf-8

# ## PrediccionDiaria_Final.ipynb
# 
# ### Objetivo
# 
# Proceso de ejecución final de prediccion mediante la llamada de las funciones desarrolladas en el módulo Funciones_Prepara_Prediccion.py
# 
# ### Descripción General de notebook
# 
#     1. Carga de funciones base
#     2. Definición y ejecución de procedimiento main()

# In[ ]:


import sys
sys.path.insert(0, '../1. Librerias Mongo')

from MongoDB_Connections import _connect_mongo
from Funciones_Prepara_Prediccion import _lecturaDatosOrigen
from Funciones_Prepara_Prediccion import _EjecutaPrediccion
from Funciones_Prepara_Prediccion import _PredicionesRanking

import time
import datetime
from datetime import date

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)


# In[ ]:


def main(argv):
        
    fechaIni = datetime.datetime.today() + datetime.timedelta(days=1)
        
    diasPrediccion = argv[1]
    
    print(fechaIni)
    print(diasPrediccion)

    t_ini = time.time()

    datosOrg = _lecturaDatosOrigen("../../Data/DataFrame_Final_Cierre_Cluster_2017_2019.csv")
    predOut = _EjecutaPrediccion(fechaIni.strftime('%d/%m/%Y'), diasPrediccion, datosOrg)
    preds = _PredicionesRanking(predOut, datosOrg)
    
    preds.to_csv('../../data/PrediccfionFinal.csv', index=False)
    
    t_end = time.time()
    
    print((t_end-t_ini)/60)


# In[ ]:


if __name__ == '__main__':
    main(sys.argv)

