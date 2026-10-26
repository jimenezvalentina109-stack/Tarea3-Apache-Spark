
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, desc

# 1. Crear la sesión de Apache Spark
spark = SparkSession.builder \
    .appName("Tarea3_Analisis_Ventas_Batch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n===== ANALISIS BATCH DE VENTAS =====")

# 2. Cargar el archivo CSV
df = spark.read.csv(
    "retail_sales_dataset.csv",
    header=True,
    inferSchema=True
)

print("\n--- Esquema de los datos ---")
df.printSchema()

print("\n--- Primeros registros ---")
df.show(5, truncate=False)

print("\nTotal de registros originales:", df.count())

# 3. Limpiar datos: eliminar filas con valores nulos
df_limpio = df.dropna()

# Eliminar posibles duplicados
df_limpio = df_limpio.dropDuplicates()

print("\nTotal de registros después de limpiar:", df_limpio.count())

# 4. Transformación: calcular ventas por categoría
ventas_categoria = df_limpio.groupBy("Product Category").agg(
    sum("Total Amount").alias("Ventas_Totales"),
    avg("Total Amount").alias("Venta_Promedio"),
    count("*").alias("Numero_Ventas")
).orderBy(desc("Ventas_Totales"))

print("\n--- Ventas por categoría ---")
ventas_categoria.show(truncate=False)

# 5. Analizar ventas por género
print("\n--- Ventas por género ---")
df_limpio.groupBy("Gender").agg(
    sum("Total Amount").alias("Ventas_Totales"),
    count("*").alias("Numero_Ventas")
).show(truncate=False)

# 6. Analizar cantidades vendidas por categoría
print("\n--- Cantidad total por categoría ---")
df_limpio.groupBy("Product Category").agg(
    sum("Quantity").alias("Cantidad_Total")
).show(truncate=False)

# 7. Crear un RDD y aplicar transformaciones
rdd_ventas = df_limpio.select("Product Category", "Total Amount").rdd

rdd_filtrado = rdd_ventas.filter(
    lambda fila: fila["Total Amount"] >= 100
)

print("\nVentas de 100 o más, usando RDD:", rdd_filtrado.count())

# 8. Guardar resultados del análisis
ventas_categoria.write.mode("overwrite").option(
    "header", True
).csv("resultados_ventas_categoria")

print("\nResultados guardados en: resultados_ventas_categoria")
print("\n===== ANALISIS FINALIZADO =====")

spark.stop()
