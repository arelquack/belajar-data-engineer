from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, concat, floor
import time

# Inisialisasi
spark = SparkSession.builder \
    .appName("Spark_Advanced_Tuning") \
    .master("local[*]") \
    .getOrCreate()

file_sumber = 'dags/project/data/transaksi_cdr.csv'
df = spark.read.csv(file_sumber, header=True, inferSchema=True)

print("=== CACHING ===")
df_bersih = df.na.drop().filter(col("pemakaian_mb") > 0)

# Nah ini dia caching-nya.
df_bersih.cache()

# Butuh waktu karena harus baca CSV dari harddisk dan nge-filter dulu
print("Memanggil Action 1 (Membaca CSV)...")
start_1 = time.time()
print(f"Jumlah data: {df_bersih.count()}")
print(f"Waktu eksekusi 1: {time.time() - start_1:.3f} detik")

# Bakal super kilat karena datanya sudah nongkrong di RAM
print("Memanggil Action 2 (Mengambil dari RAM)...")
start_2 = time.time()
print(f"Jumlah data: {df_bersih.count()}")
print(f"Waktu eksekusi 2: {time.time() - start_2:.3f} detik")

print("Salting (Skew)")

# GroupBy 'tanggal_waktu'
# Beri "Garam" (angka acak 0-9) ke kunci agregasi agar transaksi di detik yang sama terpecah ke 10 pekerja/node berbeda
df_salted = df_bersih.withColumn("garam", floor(rand() * 10)) \
                    .withColumn("salted_key", concat(col("tanggal_waktu"), lit("_"), col("garam")))

print("Bentuk data setelah digarami (Perhatikan kolom salted_key):")
df_salted.select("tanggal_waktu", "garam", "salted_key", "pemakaian_mb").show(5, truncate=False)

# Agregasi 1 - Subtotal
# Pekerja tidak akan pingsan karena datanya sudah terbagi rata ke kunci-kunci bergaram
df_agregasi_1 = df_salted.groupBy("salted_key", "tanggal_waktu") \
                        .sum("pemakaian_mb") \
                        .withColumnRenamed("sum(pemakaian_mb)", "sub_total")

# Agregasi 2 - Grandtotal
# Hasilnya sudah jauh lebih sedikit, jadi aman digabung lagi pakai kunci aslinya
df_final = df_agregasi_1.groupBy("tanggal_waktu").sum("sub_total")

print("Hasil agregasi final setelah dikumpulkan kembali:")
df_final.show(5)

spark.stop()