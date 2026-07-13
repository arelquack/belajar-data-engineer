import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')

print("Memulai proses Load ke Database...")

print("Memuat tabel pelanggan...")
df_pelanggan = pd.read_csv('project/data/pelanggan.csv')

df_pelanggan.to_sql(name='dim_pelanggan', con=engine, if_exists='replace', index=False)
print("Tabel dim_pelanggan berhasil dibuat!")

print("Memuat tabel transaksi_cdr...")
file_transaksi = 'project/data/transaksi_bersih.csv'

for chunk in pd.read_csv(file_transaksi, chunksize=10000):
    chunk['tanggal_waktu'] = pd.to_datetime(chunk['tanggal_waktu'])

    chunk.to_sql(name='fact_transaksi_cdr', con=engine, if_exists='append', index=False)

    print(f"Berhasil memuat {len(chunk)} baris transaksi.")

print("Semua data berhasil masuk ke postgreSQL! Proses Load ke Database selesai.")