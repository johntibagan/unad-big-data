# Importamos las librerías necesarias 
from pyspark.sql import SparkSession, functions as F 

# Inicializa la sesión de Spark 
spark = SparkSession.builder.appName('Tarea3').getOrCreate() 

# Define la ruta del archivo .csv en HDFS 
file_path = 'hdfs://localhost:9000/Tarea3/time_wasters_on_social_media.csv' 

# Carga el archivo .csv 
print("Cargando el dataset desde la fuente original...")
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path) 

# Imprime el esquema del DataFrame 
print("\nEsquema del DataFrame:")
df.printSchema() 

# Muestra las primeras filas del DataFrame 
print("\nPrimeras filas del DataFrame:")
df.show()

# Estadísticas básicas 
print("\nEstadísticas básicas del DataFrame:")
df.summary().show()

# 1. Limpieza de datos
print("\nEliminando duplicados y valores nulos...")
df = df.dropDuplicates()
df = df.na.drop()  # Elimina filas con valores nulos

# 2. Transformación de columnas específicas
print("\nTransformando columnas booleanas y ajustando tipos de datos...")
# Convertimos columnas booleanas y corregimos tipos en otras columnas
df = df.withColumn("Owns Property", F.when(F.col("Owns Property") == "TRUE", True).otherwise(False))
df = df.withColumn("Income", F.col("Income").cast("double"))
df = df.withColumn("Debt", F.when(F.col("Debt") == "TRUE", 1.0).otherwise(F.col("Debt").cast("double")))
df = df.withColumn("Total Time Spent", F.col("Total Time Spent").cast("double"))
df = df.withColumn("ProductivityLoss", F.col("ProductivityLoss").cast("double"))

# 3. Análisis Exploratorio de Datos (EDA)

# - Conteo de usuarios por plataforma ordenado de mayor a menor
print("\nConteo de usuarios por plataforma:")
user_count_by_platform = df.groupBy("Platform").agg(F.count("UserID").alias("User Count")) \
    .orderBy(F.desc("User Count"))
user_count_by_platform.show()

# - Conteo de usuarios por ubicación geográfica ordenado de mayor a menor
print("\nConteo de usuarios por ubicación geográfica:")
user_count_by_location = df.groupBy("Location").agg(F.count("UserID").alias("User Count")) \
    .orderBy(F.desc("User Count"))
user_count_by_location.show()

# - Segmento de usuarios que más usan alguna plataforma
print("\nSegmento de usuarios que más usan alguna plataforma:")
most_active_users = df.groupBy("Platform").agg(F.count("UserID").alias("User Count")) \
    .orderBy(F.desc("User Count")).limit(1)
most_active_users.show()

# - Impacto en la productividad (promedio de Pérdida de Productividad por plataforma)
print("\nImpacto en la productividad (promedio de Pérdida de Productividad por plataforma):")
productivity_impact = df.groupBy("Platform").agg(F.avg("ProductivityLoss").alias("Average Productivity Loss")) \
    .orderBy(F.desc("Average Productivity Loss"))
productivity_impact.show()

# - Tiempo de uso promedio gastado por los usuarios por plataforma
print("\nTiempo de uso promedio gastado por los usuarios por plataforma:")
average_time_spent = df.groupBy("Platform").agg(F.avg("Total Time Spent").alias("Average Time Spent")) \
    .orderBy(F.desc("Average Time Spent"))
average_time_spent.show()

# 4. Almacenamiento de resultados procesados
print("\nGuardando resultados procesados en formato Parquet...")
# Guarda el resultado procesado en formato Parquet para almacenamiento eficiente
processed_path = '/home/ubuntu/processed_data'
df.write.mode('overwrite').parquet(processed_path)

# Finalizando script
print("\nProcesamiento completado y datos almacenados exitosamente.")
