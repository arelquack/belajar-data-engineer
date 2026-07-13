import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')

print("Mengeksekusi Query Analitik...")

query = """
    SELECT
        p.nama,
        p.kota,
        p.paket_langganan,
        SUM(t.pemakaian_mb) AS total_pemakaian_mb
    FROM dim_pelanggan p

    -- Gabungkan (JOIN) tabel dim_pelanggan (p) dengan tabel fact_transaksi_cdr (t)
    JOIN fact_transaksi_cdr t ON p.user_id = t.user_id

    -- Filter hanya untuk pelanggan dengan paket 'Premium'
    WHERE p.paket_langganan = 'Premium'

    -- Kelompokkan data (GROUP BY) berdasarkan nama, kota, dan paket
    GROUP BY p.nama, p.kota, p.paket_langganan

    -- Urutkan dari total_pemakaian_mb paling besar ke kecil (DESC)
    ORDER BY total_pemakaian_mb DESC

    -- Batasi hanya 10 orang teratas
    LIMIT 10;
"""

df_hasil = pd.read_sql(query, con=engine)

print("\n=== TOP 10 PELANGGAN PREMIUM DENGAN PEMAKAIAN TERTINGGI ===")
print(df_hasil)

df_hasil.to_csv('project/data/datamart_top10_premium.csv', index=False)
print("\nData Mart berhasil disimpan ke CSV untuk diserahkan ke tim Marketing!")