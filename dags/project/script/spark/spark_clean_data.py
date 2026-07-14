from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Menyalakan sang manajer (driver) dengan membuat aplikasi spark yang berjalan di mode 'local' menggunakan semua core CPU laptop (ditandai dengan local[*])
spark = SparkSession.builder \
    .appName("Telecom_CDR_Cleaning") \
    .master("local[*]") \
    .getOrCreate()

print("Membaca data CDR...")
file_sumber = 'dags/project/data/transaksi_cdr.csv'

# TRANSFORMASI (Lazy Evaluation - Spark hanya mencatat, tidak memuat data ke RAM)
# Kita baca CSV-nya. inferSchema=True agar spark otomatis menebak tipe data angkanya.
df_mentah = spark.read.csv(file_sumber, header=True, inferSchema=True)

# Hapus baris yang 'pemakaian_mb' nya kosong
df_bersih = df_mentah.na.drop(subset=["pemakaian_mb"])

# Filter (ambil) HANYA data yang nilai 'pemakaian_mb'-nya lebih dari 0
df_bersih = df_bersih.filter(col("pemakaian_mb") > 0)

# Filter buang baris yang tanggal_waktu-nya berisi kata "FORMAT_SALAH"
df_bersih = df_bersih.filter(~col("tanggal_waktu").contains("FORMAT_SALAH"))

print("\n=== RENCANA EKSEKUSI SPARK (DAG) ===")
df_bersih.explain()

print("\n=== MENGEKSEKUSI ACTION ===")
jumlah_baris = df_bersih.count()
print(f"Jumlah baris setelah dibersihkan: {jumlah_baris} baris")

df_bersih.show(5)

spark.stop()