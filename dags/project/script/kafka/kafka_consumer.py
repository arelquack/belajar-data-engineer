import json
from confluent_kafka import Consumer

# Konfigurasi sang pendengar, group.id adalah nama kelompok pendengar. Kafka akan mengingat sejauh mana kelompok ini sudah membaca data.
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'tim_analitik_fraud',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Tuning ke frekuensi yang tepat
consumer.subscribe(['aliran_transaksi'])

print("Mendengarkan siaran transaksi dari Kafka... (Tekan Ctrl+C untuk berhenti)\n")

try:
    while True:
        # Dengarkan udara selama 1 detik
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"[ERROR] {msg.error()}")
            continue

        # Tangkap dan proses datanya
        data_mentah = msg.value().decode('utf-8')
        data_json = json.loads(data_mentah)

        # Contoh real-time processing (mendeteksi anomali kuota besar)
        pemakaian = data_json['pemakaian_mb']
        if pemakaian > 450:
            print(f"[ALERT] Kuota Raksasa Terdeteksi! User: {data_json['user_id']} | Pakai: {pemakaian} MB")
        else:
            print(f"[NORMAL] Transaksi masuk dari {data_json['user_id']} ({pemakaian} MB)")

except KeyboardInterrupt:
    print("\nPendengar dimatikan oleh user.")
finally:
    consumer.close()