"""
============================================================
 JAKLINGKO DATA PIPELINE - UTS (VERSI FINAL & LENGKAP)
 FILE  : etl_pyspark.py
 DESC  : ETL lengkap PySpark dengan arsitektur Unified Fact Table
         EXTRACT  -> PostgreSQL + MySQL + DWH (dim_armada)
         TRANSFORM-> Join, cleaning, dedup, kolom turunan,
                     handling missing values, Surrogate Key fallback (-1)
         LOAD     -> jaklingko_dwh (MySQL Workbench)
============================================================
"""

import os
import sys
from datetime import datetime
import mysql.connector

# ─── Setup Environment Variables ──────────────────────────────────────────────
os.environ["JAVA_HOME"]    = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["PATH"]         = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["HADOOP_HOME"]  = "F:/3SI2/SEM 6/TPD/UTS/hadoop"
os.environ["hadoop.home.dir"] = "F:/3SI2/SEM 6/TPD/UTS/hadoop"

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, trim, to_timestamp, to_date,
    date_format, year, month, quarter, dayofweek,
    weekofyear, hour, count, broadcast, current_date,
    current_timestamp, row_number, datediff, coalesce
)
from pyspark.sql.types import IntegerType, ShortType, ByteType
from pyspark.sql.window import Window

BATCH_ID = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

# ============================================================
# 1. SPARK SESSION & KONFIGURASI JDBC
# ============================================================
def buat_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("JakLingko-ETL")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.jars",
                "F:/3SI2/SEM 6/TPD/UTS/mysql-connector-j-9.6.0/mysql-connector-j-9.6.0.jar,F:/3SI2/SEM 6/TPD/UTS/postgresql-42.7.11.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

MYSQL_SRC = {
    "url"      : "jdbc:mysql://localhost:3306/jaklingko_mysql?useSSL=false&serverTimezone=Asia/Jakarta",
    "user"     : "root",
    "password" : "Bintang123",
    "driver"   : "com.mysql.cj.jdbc.Driver",
}
PG_SRC = {
    "url"      : "jdbc:postgresql://localhost:5432/jaklingko_pg",
    "user"     : "postgres",
    "password" : "Bintang123",
    "driver"   : "org.postgresql.Driver",
}
MYSQL_DWH = {
    "url"      : "jdbc:mysql://localhost:3306/jaklingko_dwh?useSSL=false&serverTimezone=Asia/Jakarta",
    "user"     : "root",
    "password" : "Bintang123",
    "driver"   : "com.mysql.cj.jdbc.Driver",
}

# ============================================================
# 2. HELPER BACA & TULIS DATABASE
# ============================================================
def baca_jdbc(spark, cfg, tabel):
    """Membaca data dari sumber relasional menggunakan JDBC."""
    df = spark.read.jdbc(
        url=cfg["url"], 
        table=tabel, 
        properties={"user": cfg["user"], "password": cfg["password"], "driver": cfg["driver"]}
    )
    print(f"  [READ] {tabel}: {df.count()} baris")
    return df

def jalankan_sql(cfg, sql_cmd):
    """Mengeksekusi native SQL query langsung ke DWH."""
    conn = mysql.connector.connect(
        host="localhost", user=cfg["user"], password=cfg["password"], database="jaklingko_dwh"
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql_cmd)
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def tulis_jdbc(df, cfg, tabel, mode="append"):
    """Menulis dataframe ke MySQL DWH dengan mematikan FK check sementara jika overwrite."""
    print(f"  [WRITE] {tabel}: {df.count()} baris (Mode: {mode})")
    
    if mode == "overwrite":
        jalankan_sql(cfg, "SET FOREIGN_KEY_CHECKS=0")
        jalankan_sql(cfg, f"DELETE FROM {tabel}")
        jalankan_sql(cfg, "SET FOREIGN_KEY_CHECKS=1")
        mode = "append"
        
    df.write.jdbc(
        url=cfg["url"], 
        table=tabel, 
        mode=mode, 
        properties={"user": cfg["user"], "password": cfg["password"], "driver": cfg["driver"]}
    )

# ============================================================
# 3. FASE EXTRACT
# ============================================================
def extract(spark):
    print("\n[FASE 1] EXTRACT")
    df_rute      = baca_jdbc(spark, MYSQL_SRC, "rute")
    df_halte     = baca_jdbc(spark, MYSQL_SRC, "halte")
    df_pengguna  = baca_jdbc(spark, PG_SRC,    "pengguna")
    df_transaksi = baca_jdbc(spark, PG_SRC,    "transaksi")
    df_feedback  = baca_jdbc(spark, PG_SRC,    "feedback")
    
    # Tarik dim_armada dari DWH untuk lookup. Jika gagal (belum ada), set None.
    try: 
        dim_armada = baca_jdbc(spark, MYSQL_DWH, "dim_armada")
    except Exception as e: 
        print("  [WARN] Tabel dim_armada belum ada di DWH, bypass lookup armada.")
        dim_armada = None
        
    return df_rute, df_halte, df_pengguna, df_transaksi, df_feedback, dim_armada

# ============================================================
# 4. FASE TRANSFORM: FUNGSI PEMBERSIHAN & DERIVASI
# ============================================================
def bersihkan(df, kolom_string: list):
    """Menghapus spasi berlebih pada kolom string."""
    for k in kolom_string:
        if k in df.columns: 
            df = df.withColumn(k, trim(col(k)))
    return df

def tentukan_jenis_koridor(df):
    """Melakukan Regex untuk menentukan jenis koridor berdasarkan Pola ID."""
    df = df.withColumn(
        "jenis_koridor",
        when(col("rute_id").rlike(r"^JAK\."), lit("Jak Lingko"))
        .when(col("rute_id").rlike(r"^R\d"),   lit("Royaltrans"))
        .when(col("rute_id").rlike(r"^M\d"),   lit("Mikrotrans"))
        .when(col("rute_id").rlike(r"^\d+$"),  lit("BRT Utama"))
        .when(col("rute_id").rlike(r"^\d+[A-Za-z]"), lit("BRT Cabang"))
        .otherwise(lit("Transjabodetabek"))
    )
    return df

def split_asal_tujuan(df):
    """Memecah string rute menjadi titik awal dan akhir."""
    df = df.withColumn("asal", F.trim(F.split(col("nama_rute"), r" - ").getItem(0)))
    df = df.withColumn("tujuan", F.trim(F.split(col("nama_rute"), r" - ").getItem(1)))
    return df

def tangani_missing(df):
    """
    Handling data kosong dan injeksi struktur.
    Untuk menjaga struktur Unified Fact Table, kolom yang tidak ada 
    di tabel sumber (PostgreSQL) disuntikkan secara dinamis.
    """
    # Buang data yang waktu tap-in nya kosong karena tidak bisa diproses
    df = df.dropna(subset=["waktu_tap_in"]) 
    
    # Injeksi kolom yang absen dari sumber RDBMS
    if "armada_id" not in df.columns: 
        df = df.withColumn("armada_id", lit(None).cast("string"))
    if "diskon" not in df.columns: 
        df = df.withColumn("diskon", lit(0).cast("double"))
    if "tarif" not in df.columns: 
        df = df.withColumn("tarif", col("total_bayar"))
    if "metode_bayar" not in df.columns: 
        df = df.withColumn("metode_bayar", lit(None).cast("string"))
        
    df = df.fillna({"halte_turun_id": "UNKNOWN", "armada_id": "UNKNOWN", "diskon": 0})
    return df

def deduplikasi(df, pk: str, urutan_col: str = "created_at"):
    """Deduplikasi data untuk mencegah double-entry."""
    if urutan_col not in df.columns: 
        urutan_col = "waktu_tap_in" # Fallback jika updated_at/created_at tidak ada
        
    w = Window.partitionBy(pk).orderBy(col(urutan_col).desc())
    df = df.withColumn("_rn", row_number().over(w))
    df = df.filter(col("_rn") == 1).drop("_rn")
    return df

def kolom_turunan_transaksi(df):
    """Menurunkan metrik waktu dan durasi perjalanan."""
    df = df.withColumn("waktu_tap_in", to_timestamp(col("waktu_tap_in")))
    df = df.withColumn("waktu_tap_out", to_timestamp(col("waktu_tap_out")))
    df = df.withColumn("jam_tap_in", hour("waktu_tap_in"))
    
    df = df.withColumn(
        "sesi_hari", 
        when(col("jam_tap_in").between(5, 9), lit("Pagi"))
        .when(col("jam_tap_in").between(10, 14), lit("Siang"))
        .when(col("jam_tap_in").between(15, 18), lit("Sore"))
        .otherwise(lit("Malam"))
    )
    
    df = df.withColumn(
        "durasi_menit", 
        when(col("waktu_tap_out").isNotNull(), 
             ((F.unix_timestamp("waktu_tap_out") - F.unix_timestamp("waktu_tap_in")) / 60).cast(ShortType()))
    )
    
    df = df.withColumn("tanggal_perjalanan", to_date(col("waktu_tap_in")))
    df = df.withColumn("is_sukses", when(col("status") == "Sukses", lit(1)).otherwise(lit(0)))
    df = df.withColumn("is_gratis", when(col("status") == "Gratis", lit(1)).otherwise(lit(0)))
    return df

def kolom_turunan_pengguna(df):
    """Menghitung usia dari tahun lahir dan membuat segmentasi."""
    df = df.withColumn("usia", (year(current_date()) - col("tahun_lahir")).cast(IntegerType()))
    
    df = df.withColumn(
        "kelompok_usia", 
        when(col("usia") < 20, lit("< 20 Tahun"))
        .when(col("usia") < 31, lit("20-30 Tahun"))
        .when(col("usia") < 41, lit("31-40 Tahun"))
        .when(col("usia") < 55, lit("41-54 Tahun"))
        .otherwise(lit(">= 55 Tahun"))
    )
    
    df = df.withColumn(
        "segment_pengguna", 
        when(col("usia") < 20, lit("Pelajar"))
        .when(col("usia") < 25, lit("Mahasiswa"))
        .when(col("usia") < 55, lit("Pekerja"))
        .otherwise(lit("Lansia"))
    )
    
    # Fallback wilayah jika kolom domisili tidak ada di database PostgreSQL
    if "kota_domisili" in df.columns:
        df = df.withColumn("wilayah",
                      when(col("kota_domisili").isin(
                          ["Jakarta Pusat", "Jakarta Selatan", "Jakarta Barat",
                           "Jakarta Utara", "Jakarta Timur", "Bogor", "Depok",
                           "Tangerang", "Bekasi"]), lit("Jabodetabek"))
                      .otherwise(lit("Lainnya")))
    else:
        df = df.withColumn("wilayah", lit("Tidak Diketahui"))
        
    df = df.drop("usia")
    return df

def buat_dim_waktu(spark, tgl_awal="2024-01-01", tgl_akhir="2024-12-31"):
    """Membangun programmatically kalender DWH."""
    date_df = spark.sql(f"SELECT explode(sequence(to_date('{tgl_awal}'), to_date('{tgl_akhir}'), interval 1 day)) AS tanggal")
    
    dim_w = date_df.withColumn("tahun", year("tanggal").cast(ShortType()))
    dim_w = dim_w.withColumn("kuartal", quarter("tanggal").cast(ByteType()))
    dim_w = dim_w.withColumn("bulan", month("tanggal").cast(ByteType()))
    dim_w = dim_w.withColumn("nama_bulan", date_format("tanggal", "MMMM"))
    dim_w = dim_w.withColumn("minggu_dalam_tahun", weekofyear("tanggal").cast(ByteType()))
    dim_w = dim_w.withColumn("hari_dalam_bulan", F.dayofmonth("tanggal").cast(ByteType()))
    dim_w = dim_w.withColumn("hari_dalam_minggu", when(dayofweek("tanggal") == 1, lit(7)).otherwise((dayofweek("tanggal") - 1).cast(ByteType())))
    dim_w = dim_w.withColumn("nama_hari", date_format("tanggal", "EEEE"))
    dim_w = dim_w.withColumn("is_weekend", when(dayofweek("tanggal").isin([1, 7]), lit(1)).otherwise(lit(0)))
    dim_w = dim_w.withColumn("is_hari_libur", lit(0))
    dim_w = dim_w.withColumn("keterangan_libur", lit(None).cast("string"))
    dim_w = dim_w.withColumn("waktu_key", row_number().over(Window.orderBy("tanggal")))
    return dim_w

def siapkan_dim_feedback_kategori(spark):
    """Injeksi data statis kategori feedback dari PySpark ke DWH menggunakan SQL JVM murni."""
    return spark.sql("""
        SELECT CAST(1 AS INT) AS feedback_kategori_key, CAST('Aplikasi Jaklingko' AS STRING) AS kategori_nama, CAST('Feedback tentang aplikasi mobile JakLingko' AS STRING) AS deskripsi UNION ALL
        SELECT CAST(2 AS INT), CAST('Fasilitas Busway' AS STRING), CAST('Feedback tentang fasilitas halte dan armada' AS STRING) UNION ALL
        SELECT CAST(3 AS INT), CAST('Umum' AS STRING), CAST('Feedback umum tentang layanan secara keseluruhan' AS STRING)
    """)

def tambah_sk(df, nama_sk, urut_by):
    """Membuat Surrogate Key terurut."""
    return df.withColumn(nama_sk, row_number().over(Window.orderBy(urut_by)))

# ============================================================
# 5. FASE TRANSFORM: BANGUN FACT TABLE
# ============================================================
def bangun_fact(df_trx, df_halte_dim, df_rute_dim, df_pengguna_dim, df_dim_waktu, df_feedback, dim_feedback_kategori, dim_armada):
    # Prefix kolom halte agar tidak konflik saat dilacak 2 kali (naik & turun)
    kolom_halte = df_halte_dim.columns
    df_halte_naik = df_halte_dim.select([col(c).alias(f"hn_{c}") for c in kolom_halte])
    df_halte_turun = df_halte_dim.select([col(c).alias(f"ht_{c}") for c in kolom_halte])

    # Resolusi pre-join untuk feedback untuk mencegah nilai key NULL di join rantai utama
    fb_kolom = ["transaksi_id", "rating", "komentar"]
    if "kategori" in df_feedback.columns:
        fb_kolom.append("kategori")
        
    df_fb_raw = df_feedback.select([col(c) for c in fb_kolom])
    if "kategori" in df_feedback.columns:
        df_fb_with_key = df_fb_raw.join(
            broadcast(dim_feedback_kategori), 
            df_fb_raw["kategori"] == dim_feedback_kategori["kategori_nama"], 
            "left"
        ).select(df_fb_raw["transaksi_id"], df_fb_raw["rating"], df_fb_raw["komentar"], dim_feedback_kategori["feedback_kategori_key"])
    else:
        df_fb_with_key = df_fb_raw.withColumn("feedback_kategori_key", lit(None).cast(IntegerType()))

    # Join Rantai Utama Transaksi dengan seluruh Dimensi
    fact = df_trx.alias("t")
    fact = fact.join(broadcast(df_dim_waktu.alias("w")), col("t.tanggal_perjalanan") == col("w.tanggal"), "left")
    fact = fact.join(broadcast(df_halte_naik), col("t.halte_naik_id") == col("hn_halte_id"), "left")
    fact = fact.join(broadcast(df_halte_turun), col("t.halte_turun_id") == col("ht_halte_id"), "left")
    fact = fact.join(broadcast(df_rute_dim.alias("r")), col("t.rute_id") == col("r.rute_id"), "left")
    fact = fact.join(df_pengguna_dim.alias("p"), col("t.pengguna_id") == col("p.pengguna_id"), "left")
    fact = fact.join(df_fb_with_key.alias("fb"), col("t.transaksi_id") == col("fb.transaksi_id"), "left")
    
    # Lookup ke dim_armada DWH jika tabelnya tersedia
    if dim_armada is not None: 
        fact = fact.join(broadcast(dim_armada.alias("a")), col("t.armada_id") == col("a.armada_id"), "left")
        armada_col = col("a.armada_key")
    else: 
        armada_col = lit(None).cast(IntegerType()).alias("armada_key")

    # Final Select Columns & Handling Missing Dimension (-1)
    fact = fact.select(
        coalesce(col("w.waktu_key"), lit(-1)).alias("waktu_key"),
        coalesce(col("hn_halte_key"), lit(-1)).alias("halte_naik_key"),
        coalesce(col("ht_halte_key"), lit(-1)).alias("halte_turun_key"),
        coalesce(col("r.rute_key"), lit(-1)).alias("rute_key"),
        coalesce(col("p.pengguna_key"), lit(-1)).alias("pengguna_key"),
        coalesce(col("fb.feedback_kategori_key"), lit(-1)).alias("feedback_kategori_key"),
        coalesce(armada_col, lit(-1)).alias("armada_key"), 
        col("t.transaksi_id"),
        col("t.armada_id"),
        col("t.waktu_tap_in"),
        col("t.waktu_tap_out"),
        col("t.jam_tap_in"),
        col("t.sesi_hari"),
        col("t.total_bayar"),
        col("t.durasi_menit"),
        col("t.tarif"),
        col("t.diskon"),
        lit(None).cast("double").alias("jarak_tempuh_km"),  # Kosongkan karena tidak ada di PG
        col("t.arah_perjalanan"),
        col("t.urutan_naik"),
        col("t.urutan_turun"),
        col("p.bank_kartu"),
        col("t.metode_bayar"),
        col("t.status").alias("status_transaksi"),
        col("t.is_sukses"),
        col("t.is_gratis"),
        col("fb.rating"),
        col("fb.komentar"),
        when(col("fb.rating").isNotNull(), lit(1)).otherwise(lit(0)).alias("has_feedback"),
        lit("postgresql").alias("sumber_data"),
        lit(BATCH_ID).alias("etl_batch_id"),
        current_timestamp().alias("etl_loaded_at")
    )
    return fact

# ============================================================
# 6. FASE LOAD KE DWH
# ============================================================
def load(dim_waktu, df_halte_dim, df_rute_dim, df_pengguna_dim, dim_feedback_kategori, df_fact):
    print("\n[FASE 3] LOAD -> jaklingko_dwh (MySQL Workbench)")
    
    # Hapus Fact table lebih dulu untuk keamanan FK
    jalankan_sql(MYSQL_DWH, "DELETE FROM fact_transaksi_perjalanan")

    # Load Dimensi Waktu
    tulis_jdbc(
        dim_waktu.select("waktu_key","tanggal","tahun","kuartal","bulan","nama_bulan","minggu_dalam_tahun","hari_dalam_bulan","hari_dalam_minggu","nama_hari","is_weekend","is_hari_libur","keterangan_libur"), 
        MYSQL_DWH, "dim_waktu", "overwrite"
    )
    
    # Load Dimensi Halte
    tulis_jdbc(
        df_halte_dim.withColumn("valid_dari", current_date()).withColumn("valid_sampai", lit(None).cast("date")).withColumn("is_current", lit(1))
        .select("halte_key","halte_id","nama_halte","latitude","longitude","valid_dari","valid_sampai","is_current"), 
        MYSQL_DWH, "dim_halte", "overwrite"
    )
    
    # Load Dimensi Rute
    tulis_jdbc(
        df_rute_dim.withColumn("valid_dari", current_date()).withColumn("valid_sampai", lit(None).cast("date")).withColumn("is_current", lit(1))
        .select("rute_key","rute_id","nama_rute","asal","tujuan","jenis_koridor","valid_dari","valid_sampai","is_current"), 
        MYSQL_DWH, "dim_rute", "overwrite"
    )
    
    # Load Dimensi Pengguna
    tulis_jdbc(
        df_pengguna_dim.withColumn("valid_dari", current_date()).withColumn("valid_sampai", lit(None).cast("date")).withColumn("is_current", lit(1))
        .select("pengguna_key","pengguna_id","nama_lengkap","bank_kartu","gender","tahun_lahir","kelompok_usia","segment_pengguna","valid_dari","valid_sampai","is_current"), 
        MYSQL_DWH, "dim_pengguna", "overwrite"
    )
    
    # Load Dimensi Feedback
    tulis_jdbc(
        dim_feedback_kategori.select("feedback_kategori_key", "kategori_nama", "deskripsi"), 
        MYSQL_DWH, "dim_feedback_kategori", "overwrite"
    )

    # ─── INJEKSI OTOMATIS DATA UNKNOWN (-1) ──────────────────────────────────
    print("  [STEP] Injeksi ulang baris Unknown Dimension (-1) ke DWH...")
    jalankan_sql(MYSQL_DWH, "INSERT IGNORE INTO dim_waktu (waktu_key, tanggal, tahun, kuartal, bulan, nama_bulan, minggu_dalam_tahun, hari_dalam_bulan, hari_dalam_minggu, nama_hari) VALUES (-1, '1900-01-01', 1900, 1, 1, 'Unknown', 1, 1, 1, 'Unknown')")
    jalankan_sql(MYSQL_DWH, "INSERT IGNORE INTO dim_halte (halte_key, halte_id, nama_halte, latitude, longitude, valid_dari) VALUES (-1, 'UNKNOWN', 'Halte Tidak Diketahui', 0, 0, '1900-01-01')")
    jalankan_sql(MYSQL_DWH, "INSERT IGNORE INTO dim_rute (rute_key, rute_id, nama_rute, jenis_koridor, valid_dari) VALUES (-1, 'UNKNOWN', 'Rute Tidak Diketahui', 'Unknown', '1900-01-01')")
    jalankan_sql(MYSQL_DWH, "INSERT IGNORE INTO dim_pengguna (pengguna_key, pengguna_id, nama_lengkap, bank_kartu, gender, kelompok_usia, segment_pengguna, valid_dari) VALUES (-1, -1, 'Tidak Diketahui', 'Unknown', 'U', 'Unknown', 'Unknown', '1900-01-01')")
    jalankan_sql(MYSQL_DWH, "INSERT IGNORE INTO dim_armada (armada_key, armada_id, rute_id, arah_perjalanan, tanggal_operasi, jenis_armada, kapasitas) VALUES (-1, 'UNKNOWN', 'UNKNOWN', 0, '1900-01-01', 'Unknown', 0)")
    jalankan_sql(MYSQL_DWH, "INSERT IGNORE INTO dim_feedback_kategori (feedback_kategori_key, kategori_nama, deskripsi) VALUES (-1, 'Unknown', 'Tidak ada feedback')")
    
    # Load Tabel Fakta (Terakhir)
    tulis_jdbc(df_fact, MYSQL_DWH, "fact_transaksi_perjalanan", "append")

# ============================================================
# 7. PIPELINE UTAMA
# ============================================================
def jalankan():
    print("=" * 60)
    print(f" JakLingko ETL PySpark | Batch: {BATCH_ID}")
    print("=" * 60)
    
    spark = buat_spark()

    # ── EXTRACT ──────────────────────────────────────────────────────────────
    df_rute, df_halte, df_pengguna, df_transaksi, df_feedback, dim_armada = extract(spark)

    # ── TRANSFORM ────────────────────────────────────────────────────────────
    print("\n[FASE 2] TRANSFORM")
    df_rute = tentukan_jenis_koridor(df_rute)
    df_rute = split_asal_tujuan(df_rute)
    df_rute = bersihkan(df_rute, ["nama_rute", "asal", "tujuan"])
    df_halte = bersihkan(df_halte, ["nama_halte"])

    df_transaksi = tangani_missing(df_transaksi)
    df_transaksi = deduplikasi(df_transaksi, "transaksi_id", "created_at")
    df_transaksi = kolom_turunan_transaksi(df_transaksi)
    df_pengguna  = kolom_turunan_pengguna(df_pengguna)

    dim_waktu = buat_dim_waktu(spark, "2024-01-01", "2024-12-31")
    df_halte_dim    = tambah_sk(df_halte,    "halte_key",    "halte_id")
    df_rute_dim     = tambah_sk(df_rute,     "rute_key",     "rute_id")
    df_pengguna_dim = tambah_sk(df_pengguna, "pengguna_key", "pengguna_id")
    dim_feedback_kategori = siapkan_dim_feedback_kategori(spark)

    df_fact = bangun_fact(df_transaksi, df_halte_dim, df_rute_dim, df_pengguna_dim, dim_waktu, df_feedback, dim_feedback_kategori, dim_armada)
    print(f"  [FACT] Siap dimuat: {df_fact.count()} baris")

    # ── LOAD ─────────────────────────────────────────────────────────────────
    load(dim_waktu, df_halte_dim, df_rute_dim, df_pengguna_dim, dim_feedback_kategori, df_fact)
    
    print("\n" + "=" * 60)
    print(f" Pipeline selesai: {BATCH_ID}")
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    jalankan()