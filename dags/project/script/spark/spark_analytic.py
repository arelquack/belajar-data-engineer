from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc

# Inisialiasi sang manajer
spark = SparkSession.builder \
    .appName("Telecom_Spark_Analytics") \
    .master("local[*]") \
    .getOrCreate()

file_transaksi = 'dags/project/data/transaksi_cdr.csv'
file_pelanggan = 'dags/project/data/pelanggan.csv'

print("Membaca data...")
df_transaksi = spark.read.csv(file_transaksi, header=True, inferSchema=True)
df_pelanggan = spark.read.csv(file_pelanggan, header=True, inferSchema=True)

# Transformasi Narrow (tidak butuh shuffling)
df_transaksi_bersih = df_transaksi.na.drop(subset=["pemakaian_mb"]) \
    .filter(col("pemakaian_mb") > 0) \
    .filter(~col("tanggal_waktu").contains("FORMAT_SALAH"))

# Filter hanya pelanggan "Premium"
df_pelanggan_premium = df_pelanggan.filter(col("paket_langganan") == "Premium")

# Transformasi Wide (butuh shuffling)
# Melakukan INNER JOIN antara transaksi bersih dan pelanggan premium
df_joined = df_transaksi_bersih.join(df_pelanggan_premium, on="user_id", how="inner")

# Melakukan GROUP BY nama dan kota, lalu di-SUM
df_agregasi = df_joined.groupBy("nama", "kota") \
    .agg(spark_sum("pemakaian_mb").alias("total_pemakaian_mb"))

# ORDER BY dan LIMIT
df_top10 = df_agregasi.orderBy(desc("total_pemakaian_mb")).limit(10)

print("\n=== RENCANA EKSEKUSI (EXPLAIN) ===")
df_top10.explain()

print("\n=== HASIL TOP 10 PELANGGAN PREMIUM ===")
df_top10.show(truncate=False)

spark.stop()