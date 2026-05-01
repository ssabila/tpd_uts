"""
============================================================
 JAKLINGKO DATA PIPELINE – UTS
 FILE  : etl_pyspark.py
 DESC  : ETL PySpark disesuaikan dengan dataset Kaggle
         dfTransjakarta.csv (April 2023)

 EXTRACT  → jaklingko_mysql (rute, halte)
            jaklingko_pg    (pengguna, transaksi)
 TRANSFORM→ join, cleaning, kolom turunan sesuai Kaggle
 LOAD     → jaklingko_dwh  (MySQL Workbench)

 Jalankan:
   spark-submit \
     --packages mysql:mysql-connector-java:8.0.33,\
                org.postgresql:postgresql:42.6.0 \
     etl_pyspark.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, trim, to_timestamp, to_date,
    date_format, year, month, quarter, dayofweek,
    weekofyear, hour, current_date, current_timestamp,
    row_number, broadcast, regexp_extract, split
)
from pyspark.sql.types import ShortType, IntegerType, DecimalType
from pyspark.sql.window import Window
from datetime import datetime

BATCH_ID = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

# ============================================================
# 1. SPARK SESSION
# ============================================================
def buat_spark():
    spark = (
        SparkSession.builder
        .appName("JakLingko-ETL-Kaggle")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================================================
# 2. KONFIGURASI JDBC
# ============================================================
MYSQL_SRC = {
    "url"    : "jdbc:mysql://localhost:3306/jaklingko_mysql?useSSL=false&serverTimezone=Asia/Jakarta",
    "user"   : "jaklingko_user",
    "password": "jaklingko_pass",
    "driver" : "com.mysql.cj.jdbc.Driver",
}
PG_SRC = {
    "url"    : "jdbc:postgresql://localhost:5432/jaklingko_pg",
    "user"   : "jaklingko_user",
    "password": "jaklingko_pass",
    "driver" : "org.postgresql.Driver",
}
MYSQL_DWH = {
    "url"    : "jdbc:mysql://localhost:3306/jaklingko_dwh?useSSL=false&serverTimezone=Asia/Jakarta",
    "user"   : "dwh_user",
    "password": "dwh_pass",
    "driver" : "com.mysql.cj.jdbc.Driver",
}

# ============================================================
# 3. HELPER JDBC
# ============================================================
def baca_jdbc(spark, cfg, tabel, query=None):
    r = (spark.read.format("jdbc")
         .option("url", cfg["url"]).option("user", cfg["user"])
         .option("password", cfg["password"]).option("driver", cfg["driver"])
         .option("fetchsize", "1000"))
    r = r.option("dbtable", f"({query}) AS sub") if query else r.option("dbtable", tabel)
    df = r.load()
    print(f"  [READ] {tabel}: {df.count()} baris")
    return df

def tulis_jdbc(df, cfg, tabel, mode="append"):
    n = df.count()
    print(f"  [WRITE] {tabel}: {n} baris (mode={mode})")
    (df.write.format("jdbc")
       .option("url", cfg["url"]).option("dbtable", tabel)
       .option("user", cfg["user"]).option("password", cfg["password"])
       .option("driver", cfg["driver"]).option("batchsize", "1000")
       .mode(mode).save())
    print(f"  [OK] {tabel}")

# ============================================================
# 4. EXTRACT
# ============================================================
def extract(spark):
    print("\n[FASE 1] EXTRACT")
    df_rute  = baca_jdbc(spark, MYSQL_SRC, "rute")
    df_halte = baca_jdbc(spark, MYSQL_SRC, "halte")
    df_pengguna  = baca_jdbc(spark, PG_SRC, "pengguna")
    df_transaksi = baca_jdbc(spark, PG_SRC,
        tabel="transaksi_sukses",
        query="SELECT * FROM transaksi WHERE status IN ('Sukses','Gratis')"
    )
    return df_rute, df_halte, df_pengguna, df_transaksi

# ============================================================
# 5. TRANSFORM
# ============================================================

def tentukan_jenis_koridor(df):
    """
    Klasifikasi jenis koridor dari pola corridorID (rute_id):
      Angka murni (1,2,…13)  → BRT Utama
      Angka+huruf (6C,8B…)   → BRT Cabang
      R… (R1, R1A)           → Royaltrans
      M… (M7B, M14)          → Mikrotrans
      JAK.xx                 → Jak Lingko
      Lainnya (JIS, S21…)    → Transjabodetabek
    """
    return df.withColumn(
        "jenis_koridor",
        when(col("rute_id").rlike(r"^JAK\."), lit("Jak Lingko"))
        .when(col("rute_id").rlike(r"^R\d"),   lit("Royaltrans"))
        .when(col("rute_id").rlike(r"^M\d"),   lit("Mikrotrans"))
        .when(col("rute_id").rlike(r"^\d+$"),  lit("BRT Utama"))
        .when(col("rute_id").rlike(r"^\d+[A-Za-z]"), lit("BRT Cabang"))
        .otherwise(lit("Transjabodetabek"))
    )


def split_asal_tujuan(df):
    """
    Split corridorName menjadi asal dan tujuan.
    Format: "Asal - Tujuan" atau "Asal - Tujuan via X"
    Pisah pada " - " pertama.
    """
    df = df.withColumn("asal",
        F.trim(F.split(col("nama_rute"), r" - ").getItem(0)))
    df = df.withColumn("tujuan",
        F.trim(F.split(col("nama_rute"), r" - ").getItem(1)))
    return df


def kolom_turunan_pengguna(df):
    """
    Tambah kolom turunan dari data pengguna Kaggle:
    - usia           : 2023 - tahun_lahir
    - kelompok_usia  : bin usia
    - segment_pengguna: Pelajar/Mahasiswa/Pekerja/Lansia
    """
    df = df.withColumn("usia",
        (lit(2023) - col("tahun_lahir")).cast(IntegerType()))
    df = df.withColumn("kelompok_usia",
        when(col("usia") <= 22, lit("<= 22 thn"))
        .when(col("usia") <= 32, lit("23-32 thn"))
        .when(col("usia") <= 42, lit("33-42 thn"))
        .when(col("usia") <= 52, lit("43-52 thn"))
        .otherwise(lit(">= 53 thn")))
    df = df.withColumn("segment_pengguna",
        when(col("usia") <= 17, lit("Pelajar"))
        .when(col("usia") <= 24, lit("Mahasiswa"))
        .when(col("usia") <= 55, lit("Pekerja"))
        .otherwise(lit("Lansia")))
    return df


def kolom_turunan_transaksi(df):
    """
    Tambah kolom turunan dari transaksi Kaggle:
    - jam_tap_in     : jam 0-23
    - sesi_hari      : Pagi/Siang/Sore/Malam
    - durasi_menit   : selisih tap-in dan tap-out
    - status_transaksi: Sukses/Gratis/Inkomplет
    - is_sukses, is_gratis
    - tanggal_perjalanan: untuk lookup dim_waktu
    """
    df = df.withColumn("waktu_tap_in",
        to_timestamp(col("waktu_tap_in")))
    df = df.withColumn("waktu_tap_out",
        to_timestamp(col("waktu_tap_out")))

    df = df.withColumn("jam_tap_in", hour("waktu_tap_in"))
    df = df.withColumn("sesi_hari",
        when(col("jam_tap_in").between(5, 9),   lit("Pagi"))
        .when(col("jam_tap_in").between(10, 14), lit("Siang"))
        .when(col("jam_tap_in").between(15, 18), lit("Sore"))
        .otherwise(lit("Malam")))

    df = df.withColumn("durasi_menit",
        when(col("waktu_tap_out").isNotNull(),
            ((F.unix_timestamp("waktu_tap_out") -
              F.unix_timestamp("waktu_tap_in")) / 60).cast(ShortType())))

    # Status turunan dari payAmount dan tapOutTime
    df = df.withColumn("status_transaksi",
        when(col("waktu_tap_out").isNull(), lit("Inkomplет"))
        .when(col("total_bayar") == 0,      lit("Gratis"))
        .otherwise(lit("Sukses")))

    df = df.withColumn("is_sukses",
        when(col("status_transaksi") == "Sukses", lit(1)).otherwise(lit(0)))
    df = df.withColumn("is_gratis",
        when(col("status_transaksi") == "Gratis", lit(1)).otherwise(lit(0)))

    df = df.withColumn("tanggal_perjalanan",
        to_date(col("waktu_tap_in")))
    return df


def buat_dim_waktu(spark, tgl_awal="2023-04-01", tgl_akhir="2023-04-30"):
    """Generate date spine sesuai rentang data Kaggle (April 2023)."""
    date_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{tgl_awal}'), to_date('{tgl_akhir}'),
            interval 1 day)) AS tanggal
    """)
    w = Window.orderBy("tanggal")
    dim_w = (date_df
        .withColumn("tahun",              year("tanggal").cast(ShortType()))
        .withColumn("kuartal",            F.quarter("tanggal").cast("tinyint"))
        .withColumn("bulan",              month("tanggal").cast("tinyint"))
        .withColumn("nama_bulan",         date_format("tanggal", "MMMM"))
        .withColumn("minggu_dalam_tahun", weekofyear("tanggal").cast("tinyint"))
        .withColumn("hari_dalam_bulan",   F.dayofmonth("tanggal").cast("tinyint"))
        .withColumn("hari_dalam_minggu",
            # Senin=1 … Minggu=7  (dayofweek PySpark: 1=Minggu, 7=Sabtu)
            when(dayofweek("tanggal") == 1, lit(7))
            .otherwise((dayofweek("tanggal") - 1).cast("tinyint")))
        .withColumn("nama_hari",          date_format("tanggal", "EEEE"))
        .withColumn("is_weekend",
            when(dayofweek("tanggal").isin([1, 7]), lit(1)).otherwise(lit(0)))
        .withColumn("is_hari_libur",      lit(0))
        .withColumn("keterangan_libur",   lit(None).cast("string"))
        .withColumn("waktu_key",          row_number().over(w))
    )
    return dim_w


def tambah_sk(df, sk_col, urut_by):
    w = Window.orderBy(urut_by)
    return df.withColumn(sk_col, row_number().over(w))


def bangun_fact(df_trx, df_halte_dim, df_rute_dim,
                df_pengguna_dim, dim_waktu):
    """Join transaksi ke semua dimensi → bangun fact table."""
    fact = (
        df_trx.alias("t")
        .join(broadcast(dim_waktu.alias("w")),
              col("t.tanggal_perjalanan") == col("w.tanggal"), "left")
        .join(broadcast(df_halte_dim.alias("hn")),
              col("t.halte_naik_id") == col("hn.halte_id"), "left")
        .join(broadcast(df_halte_dim.alias("ht")),
              col("t.halte_turun_id") == col("ht.halte_id"), "left")
        .join(broadcast(df_rute_dim.alias("r")),
              col("t.rute_id") == col("r.rute_id"), "left")
        .join(df_pengguna_dim.alias("p"),
              col("t.pengguna_id") == col("p.pengguna_id"), "left")
        .select(
            col("w.waktu_key"),
            col("hn.halte_key").alias("halte_naik_key"),
            col("ht.halte_key").alias("halte_turun_key"),
            col("r.rute_key"),
            col("p.pengguna_key"),
            col("t.transaksi_id"),
            col("t.waktu_tap_in"),
            col("t.waktu_tap_out"),
            col("t.jam_tap_in"),
            col("t.sesi_hari"),
            col("t.total_bayar"),
            col("t.durasi_menit"),
            col("t.arah_perjalanan"),
            col("t.urutan_naik"),
            col("t.urutan_turun"),
            # bank_kartu dari dim_pengguna sebagai proxy metode bayar
            col("p.bank_kartu"),
            col("t.status_transaksi"),
            col("t.is_sukses"),
            col("t.is_gratis"),
            lit("postgresql").alias("sumber_data"),
            lit(BATCH_ID).alias("etl_batch_id"),
            current_timestamp().alias("etl_loaded_at"),
        )
    )
    # Hanya simpan yang berhasil lookup pengguna & waktu
    fact = fact.filter(
        col("waktu_key").isNotNull() &
        col("pengguna_key").isNotNull() &
        col("halte_naik_key").isNotNull()
    )
    return fact

# ============================================================
# 6. LOAD
# ============================================================
def load(dim_waktu, df_halte_dim, df_rute_dim, df_pengguna_dim, df_fact):
    print("\n[FASE 3] LOAD → jaklingko_dwh (MySQL Workbench)")

    tulis_jdbc(
        dim_waktu.select("waktu_key","tanggal","tahun","kuartal","bulan",
                         "nama_bulan","minggu_dalam_tahun","hari_dalam_bulan",
                         "hari_dalam_minggu","nama_hari","is_weekend",
                         "is_hari_libur","keterangan_libur"),
        MYSQL_DWH, "dim_waktu", mode="overwrite"
    )
    tulis_jdbc(
        df_halte_dim.withColumn("valid_dari", current_date())
                    .withColumn("valid_sampai", lit(None).cast("date"))
                    .withColumn("is_current", lit(1))
                    .select("halte_key","halte_id","nama_halte","latitude",
                            "longitude","valid_dari","valid_sampai","is_current"),
        MYSQL_DWH, "dim_halte", mode="overwrite"
    )
    tulis_jdbc(
        df_rute_dim.withColumn("valid_dari", current_date())
                   .withColumn("valid_sampai", lit(None).cast("date"))
                   .withColumn("is_current", lit(1))
                   .select("rute_key","rute_id","nama_rute","asal","tujuan",
                           "jenis_koridor","valid_dari","valid_sampai","is_current"),
        MYSQL_DWH, "dim_rute", mode="overwrite"
    )
    tulis_jdbc(
        df_pengguna_dim.withColumn("valid_dari", current_date())
                       .withColumn("valid_sampai", lit(None).cast("date"))
                       .withColumn("is_current", lit(1))
                       .select("pengguna_key","pengguna_id","nama_lengkap",
                               "bank_kartu","gender","tahun_lahir","usia",
                               "kelompok_usia","segment_pengguna",
                               "valid_dari","valid_sampai","is_current"),
        MYSQL_DWH, "dim_pengguna", mode="overwrite"
    )
    tulis_jdbc(df_fact, MYSQL_DWH, "fact_transaksi_perjalanan", mode="append")

# ============================================================
# 7. MAIN
# ============================================================
def jalankan():
    print("=" * 60)
    print(f" JakLingko ETL PySpark – Dataset Kaggle")
    print(f" Batch ID : {BATCH_ID}")
    print("=" * 60)

    spark = buat_spark()

    # EXTRACT
    df_rute, df_halte, df_pengguna, df_transaksi = extract(spark)

    # TRANSFORM
    print("\n[FASE 2] TRANSFORM")

    # Rute: tambah kolom turunan
    df_rute = tentukan_jenis_koridor(df_rute.withColumnRenamed("rute_id","rute_id"))
    df_rute = split_asal_tujuan(df_rute)

    # Pengguna: tambah kolom usia
    df_pengguna = kolom_turunan_pengguna(df_pengguna)

    # Transaksi: kolom turunan waktu & status
    df_transaksi = kolom_turunan_transaksi(df_transaksi)

    # Deduplikasi (jaga-jaga)
    w_dedup = Window.partitionBy("transaksi_id").orderBy(col("created_at").desc())
    df_transaksi = (df_transaksi
        .withColumn("_rn", row_number().over(w_dedup))
        .filter(col("_rn") == 1).drop("_rn"))

    # Dimensi waktu
    dim_waktu = buat_dim_waktu(spark, "2023-04-01", "2023-04-30")

    # Surrogate key
    df_halte_dim    = tambah_sk(df_halte,    "halte_key",    "halte_id")
    df_rute_dim     = tambah_sk(df_rute,     "rute_key",     "rute_id")
    df_pengguna_dim = tambah_sk(df_pengguna, "pengguna_key", "pengguna_id")

    # Fact
    df_fact = bangun_fact(df_transaksi, df_halte_dim, df_rute_dim,
                          df_pengguna_dim, dim_waktu)
    print(f"  [FACT] Siap dimuat: {df_fact.count()} baris")

    # LOAD
    load(dim_waktu, df_halte_dim, df_rute_dim, df_pengguna_dim, df_fact)

    print("\n" + "=" * 60)
    print(f" Pipeline selesai: {BATCH_ID}")
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    jalankan()
