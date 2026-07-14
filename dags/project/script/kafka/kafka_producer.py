import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

# Konfigurasi sang penyiar, arahkan antena ke menara kafka yang menyala di port 9092
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

print("Memulai Siaran Transaksi ke Kafka... (Tekan Ctrl+C untuk berhenti)\n")

# Fungsi kecil untuk memastikan pesan benar-benar sampai ke Kafka
def cek_pengiriman(err, msg):
    if err is not None:
        print(f"[GAGAL] Pesan tidak terkirim: {err}")
    else:
        print(f"[TERKIRIM] Topik: {msg.topic()} | Data: {msg.value().decode('utf-8')}")

try:
    # Looping tanpa henti mensimulasikan aliran data Real-Time
    while True:
        # Generate 1 baris data transaksi palsu
        data_transaksi = {
            "trx_id": f"TRX{random.randint(100000, 999999)}",
            "user_id": f"U0{random.randint(1000, 9999)}",
            "tanggal_waktu": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "pemakaian_mb": round(random.uniform(5.0, 500.0), 2)
        }

        # Publish (siarkan) data ke udara dengan nama saluran "aliran_transaksi"
        # Karena kafka cuma paham byte, kita ubah JSON-nya jadi string lalu di-encode
        producer.produce(
            topic='aliran_transaksi',
            value=json.dumps(data_transaksi).encode('utf-8'),
            callback=cek_pengiriman
        )
        producer.poll(0) # Memicu callback

        # Jeda 1 detik per transaksi agar terminalmu tidak meledak
        time.sleep(1)

except KeyboardInterrupt:
    print("\nSiaran dihentikan oleh user.")
finally:
    producer.flush()