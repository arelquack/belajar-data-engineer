import pandas as pd
import numpy as np
from datetime import datetime, timedelta

print("Membuat data pelanggan...")
pelanggan = pd.DataFrame({
    'user_id': [f"U{str(i).zfill(5)}" for i in range(1, 10001)],
    'nama': [f"Pelanggan_{i}" for i in range(1, 10001)],
    'kota': np.random.choice(['Jakarta', 'Bandung', 'Surabaya', 'Medan', 'Makassar'], 10000),
    'paket_langganan': np.random.choice(['Basic', 'Premium', 'Pro'], 10000)
})
pelanggan.to_csv('pelanggan.csv', index=False)

print("Membuat data transaksi (CDR)...")
# Membuat 100.000 transaksi
user_ids = np.random.choice(pelanggan['user_id'], 100000)
dates = [datetime(2026, 7, 1) + timedelta(minutes=int(x)) for x in np.random.randint(0, 43200, 100000)]
usage_mb = np.random.normal(500, 200, 100000) # Pemakaian normal
usage_mb[np.random.choice(100000, 5000, replace=False)] = np.nan # Sengaja bikin 5000 data kosong (NULL)
usage_mb[np.random.choice(100000, 2000, replace=False)] = -150 # Sengaja bikin 2000 data minus (Error)

transaksi = pd.DataFrame({
    'trx_id': [f"TRX{str(i).zfill(6)}" for i in range(1, 100001)],
    'user_id': user_ids,
    'tanggal_waktu': dates,
    'pemakaian_mb': usage_mb
})
# Sengaja bikin format tanggal berantakan di beberapa baris
transaksi['tanggal_waktu'] = transaksi['tanggal_waktu'].astype(str)
transaksi.loc[0:100, 'tanggal_waktu'] = "FORMAT_SALAH_2026"

transaksi.to_csv('transaksi_cdr.csv', index=False)
print("Selesai! File pelanggan.csv dan transaksi_cdr.csv berhasil dibuat.")