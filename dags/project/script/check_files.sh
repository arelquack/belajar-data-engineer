#!/bin/bash

set -e

DIR_PATH="project/data"

if [ -f "${DIR_PATH}/pelanggan.csv" ]; then
    echo "File pelanggan.csv ditemukan!"
    echo "Jumlah baris pelanggan:"
    wc -l < "${DIR_PATH}/pelanggan.csv"
else
    echo "File pelanggan.csv tidak ditemukan!"
fi

if [ -f "${DIR_PATH}/transaksi_cdr.csv" ]; then
    echo "File transaksi_cdr.csv ditemukan!"
    echo "Jumlah baris transaksi:"
    wc -l < "${DIR_PATH}/transaksi_cdr.csv"

    echo "Baris dengan format salah:"
    grep "FORMAT_SALAH" "${DIR_PATH}/transaksi_cdr.csv" > "${DIR_PATH}/error_log.txt" || true

    echo "Pencarian selesai. Cek error_log.txt untuk melihat baris dengan format salah."
else
    echo "File transaksi_cdr.csv tidak ditemukan!"
fi