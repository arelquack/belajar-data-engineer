import pandas as pd

file_sumber = 'project/data/transaksi_cdr.csv'
file_tujuan = 'project/data/transaksi_bersih.csv'

# Flag untuk menulis header hanya di iterasi (chunk) pertama
is_first_chunk = True

print("Memulai proses pembersihan data CDR...")

# Membaca data per 10.000 baris agar tidak boros RAM
for chunk in pd.read_csv(file_sumber, chunksize=10000):

    # Hapus baris yang kolom 'pemakaian_mb'-nya kosong (NaN)
    chunk = chunk.dropna(subset=['pemakaian_mb'])

    # Filter (ambil) HANYA data yang nilai 'pemakaian_mb'-nya lebih dari 0
    chunk = chunk[chunk['pemakaian_mb'] > 0]

    # Filter (ambil) HANYA baris yang tanggal_waktu-nya TIDAK mengandung kata 'FORMAT_SALAH'
    chunk = chunk[chunk['tanggal_waktu'].str.contains('FORMAT_SALAH') == False]

    if is_first_chunk:
        # Tulis dengan header untuk chunk pertama
        chunk.to_csv(file_tujuan, index=False, mode='w', header=True)
        is_first_chunk = False
    else:
        # Tulis tanpa header untuk chunk berikutnya
        chunk.to_csv(file_tujuan, index=False, mode='a', header=False)

    print(f"Proses pembersihan data CDR selesai untuk chunk ini. Data bersih disimpan di {file_tujuan}")