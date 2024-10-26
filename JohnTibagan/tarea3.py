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
# - Distribución de la edad por plataforma
print("\nDistribución de la edad promedio por plataforma:")
age_distribution = df.groupBy("Platform").agg(F.avg("Age").alias("Average Age"))
age_distribution.show()

# - Usuarios con alto nivel de adicción y satisfacción
print("\nUsuarios con alto nivel de adicción y satisfacción (ambos mayor a 4):")
high_addiction_satisfaction = df.filter((F.col("Addiction Level") > 4) & (F.col("Satisfaction") > 4))
high_addiction_satisfaction.select("UserID", "Addiction Level", "Satisfaction").show()

# - Ingreso promedio y deuda promedio por ocupación
print("\nIngreso promedio y deuda promedio por ocupación:")
income_debt_by_profession = df.groupBy("Profession").agg(
    F.avg("Income").alias("Avg Income"),
    F.avg("Debt").alias("Avg Debt")
)
income_debt_by_profession.show()

# Usuarios menores de 28 años con algunas columnas seleccionadas
print("\nUsuarios menores de 28 años:")
ages = df.filter(F.col("Age") < 28).select('UserID', 'Age', 'Platform', 'Total Time Spent')
ages.show()

# Usuarios ordenados por edad en orden descendente
print("\nUsuarios ordenados por edad descendente:")
sorted_df = df.orderBy(F.col("Age").desc())
sorted_df.show()

# 4. Almacenamiento de resultados procesados
print("\nGuardando resultados procesados en formato Parquet...")
# Guarda el resultado procesado en formato Parquet para almacenamiento eficiente
processed_path = '/home/ubuntu/processed_data'
df.write.mode('overwrite').parquet(processed_path)

# Finalizando script
print("\nProcesamiento completado y datos almacenados exitosamente.")