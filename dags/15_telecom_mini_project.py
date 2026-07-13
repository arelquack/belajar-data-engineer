from airflow.sdk import dag, task
from pendulum import datetime

# Kita set jadwal harian dan yang paling penting: catchup=False!
@dag(
    dag_id="telecom_daily_etl",
    start_date=datetime(2026, 7, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    tags=["telecom", "mini-project"]
)

def telecom_daily_etl():

    # 1. EXTRACT: Simulasi mengambil data dari server
    @task.python
    def extract_cdr_data():
        print("Menarik data Call Detail Records (CDR) harian...")

        # Simulasi data mentah (misal hasil dari API atau query DB)
        raw_data = [
            {"user_id": "U001", "usage_mb": 1500, "network": "5G"},
            {"user_id": "U002", "usage_mb": 0, "network": "4G"},
            {"user_id": "U003", "usage_mb": 3200, "network": "4G"},
        ]

        # Bisa juga dicoba diubah jadi raw_data = [] untuk melihat rute alert berjalan
        return raw_data
    
    # 2. BRANCHING
    @task.branch
    def check_data_quality(data: list):
        print("Memvalidasi jumlah data...")
        if len(data) == 0:
            return "alert_empty_data"
        else:
            return "transform_usage_data"
        
    # 3a. Rute jika data KOSONG
    @task.python
    def alert_empty_data():
        print("CRITICAL: Data kosong! Mengirimkan notifikasi ke tim NOC...")
        return "Notifikasi terkirim"
    
    # 3b. Rute jika data ADA (TRANSFORM)
    @task.python
    def transform_usage_data(data: list):
        print("Membersihkan dan memformat data...")
        clean_data = []
        for row in data:
            if row["usage_mb"] > 0: # Buang yang pemakaiannya 0
                clean_data.append({
                    "user_id": row["user_id"],
                    "usage_gb": round(row["usage_mb"] / 1024, 2),
                    "network_type": row["network"]
                })
        return clean_data
    
    # 4. LOAD: Memasukkan data bersih ke tujuan akhir
    @task.python
    def load_to_warehouse(clean_data: list):
        print(f"Berhasil memuat {len(clean_data)} baris data ke Data Warehouse!")
        print("Sample data yang dimuat:", clean_data)
        return "SUCCESS"
    
    # ==========================================
    # MENGATUR ALUR KERJA (DEPENDENCIES)
    # ==========================================

    # Inisiasi task
    raw_cdr = extract_cdr_data()

    # Hasil extract masuk ke fungsi percabangan
    branch_decision = check_data_quality(raw_cdr)

    # Deklarasi task cabang
    alert = alert_empty_data()
    transform = transform_usage_data(raw_cdr)
    load = load_to_warehouse(transform)

    # Definisi rute panahnya
    branch_decision >> [alert, transform]

    # Kita beritahu Airflow kalau 'load' itu kelanjutan dari 'transform'
    transform >> load

telecom_daily_etl()
