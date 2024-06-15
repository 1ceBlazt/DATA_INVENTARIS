from pyspark.sql import SparkSession

# Membuat Spark session
spark = SparkSession.builder \
    .appName("Process Data Inventaris dan Transaksi") \
    .getOrCreate()

# Membaca data dari file CSV untuk data inventaris
df_inventaris = spark.read.csv("/home/achmadakbar/airflow/dags/data_inventaris_mentahh.csv", header=True, inferSchema=True)

# Melakukan pemrosesan data inventaris (contoh: filter data)
df_inventaris_filtered = df_inventaris.filter(df_inventaris['some_column'] > 100)

# Menyimpan data inventaris yang telah diproses ke file CSV lain
df_inventaris_filtered.write.csv("/home/achmadakbar/airflow/dags/data_inventaris_mentahh.csv", header=True)

# Membaca data dari file CSV untuk data transaksi customer
df_transaksi = spark.read.csv("/home/achmadakbar/airflow/dags/data_transaksi_customer.csv", header=True, inferSchema=True)

# Melakukan pemrosesan data transaksi customer (contoh: filter data)
df_transaksi_filtered = df_transaksi.filter(df_transaksi['another_column'] > 50)

# Menyimpan data transaksi customer yang telah diproses ke file CSV lain
df_transaksi_filtered.write.csv("/home/achmadakbar/airflow/dags/data_transaksi_customer.csv", header=True)

# Menghentikan Spark session
spark.stop()

