# UNAD - Curso Big Data - Grupo 21

## Ejecución en Spark

> Dataset fuente: https://www.kaggle.com/datasets/zeesolver/dark-web?resource=download

Luego de las configuraciones en la maquina virtual para adecuar Hadoop y Spark, a continuación se definen los comandos para el cargue del dataset, script y ejecución.
``` bash
#usuario hadoop:hadoop
# Crear carpeta
hdfs dfs -mkdir /Tarea3

#!/bin/bash
wget https://raw.githubusercontent.com/johntibagan/unad-big-data/refs/heads/main/JohnTibagan/time_wasters_on_social_media.csv
pwd

# mover archivo a Tarea3
hdfs dfs -put /home/hadoop/time_wasters_on_social_media.csv /Tarea3

# Archivo Python
nano tarea3.py  # copiar y pegar ccontenido del script

# Ejecutar script 
python3 tarea3.py
```