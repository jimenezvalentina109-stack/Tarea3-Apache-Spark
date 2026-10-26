# Tarea3-Apache-Spark
Este proyecto realiza un análisis por lotes (Batch) de un conjunto de datos de ventas minoristas utilizando Apache Spark y Python

Conjunto de datos
Se utiliza el conjunto de datos retail_sales_dataset.csv, que contiene información sobre transacciones de ventas, categorías de productos, género, cantidades y valores de compra.

Procesamiento realizado
Carga del archivo CSV.
Limpieza de datos y eliminación de registros duplicados.
Análisis de ventas por categoría de producto.
Análisis de ventas según el género.
Cálculo de cantidades vendidas por categoría.
Uso de RDD para filtrar ventas con valor igual o superior a 100.
Exportación de resultados a archivos CSV.

Requisitos
Python
Apache Spark
Java
El conjunto de datos retail_sales_dataset.csv

Ejecución
Ubicar el archivo batch_analysis.py y el conjunto de datos retail_sales_dataset.csv en el entorno de trabajo. Desde la terminal, ejecutar:
spark-submit batch_analysis.py

Resultados
El programa presenta estadísticas de ventas por categoría de producto y género, además de guardar los resultados del análisis en archivos CSV.

Autor
Estudiante - Tarea 3
