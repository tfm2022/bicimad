REM **********************************************************************************************************************************************
REM * EJECUCION DE PROCESO DE PREDICCION BASADOS EN MODELOS DE MACHINE LEARNING CONSTRUIDOS PARA CADA UNA DE LAS ESTACIONES DE BICIMAD
REM * PARAMETRO: NUMERO DE DIAS FUTUROS A PREDECIR A PARTIR DEL DIA SIGUIENTE DE LA FECHA ACTUAL
REM * 
REM * NOTA: LOS MODELOS ENTRENADOS (.pkl) DEBEN ESTAR UBICADOS EN LA CARPETA ../../MODELOS 
REM * 
REM **********************************************************************************************************************************************


cls


"C:\ProgramData\Anaconda3\Scripts\jupyter" nbconvert --to script "PrediccionDiaria_Final.ipynb"
"C:\ProgramData\Anaconda3\Scripts\jupyter" nbconvert --to script "Funciones_Prepara_Prediccion.ipynb"

C:\ProgramData\Anaconda3\python .\PrediccionDiaria_Final.py %1
