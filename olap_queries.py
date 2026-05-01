"""
============================================================
 JAKLINGKO DATA PIPELINE – UTS
 FILE  : olap_queries.py
 DESC  : Query OLAP terhadap DWH MySQL (jaklingko_dwh)
         disesuaikan dengan kolom dari dataset Kaggle:
           - bank_kartu  (bukan metode_bayar)
           - jenis_koridor (bukan jenis_moda)
           - arah_perjalanan (direction: 0/1)
           - is_gratis (payAmount = 0)

 Jalankan:
   spark-submit --packages mysql:mysql-connector-java:8.0.33 \
     olap_queries.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, round as _round,
    when, lit
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os

OUTPUT_DIR = "output_grafik"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MYSQL_DWH = {
    "url"     : "jdbc:mysql://localhost:3306/jaklingko_dwh?useSSL=false",
    "user"    : "dwh_user",
    "password": "dwh_pass",
    "driver"  : "com.mysql.cj.jdbc.Driver",
}
WARNA = ["#1a6bb5","#2eb87e","#e85d2e","#f5a623",
         "#9b59b6","#34495e","#1abc9c","#e74c3c"]


def buat_spark():
    s = (SparkSession.builder.appName("JakLingko-OLAP-Kaggle")
         .master("local[*]").getOrCreate())
    s.sparkContext.setLogLevel("WARN")
    return s


def baca(spark, tabel):
    return (spark.read.format("jdbc")
            .option("url",      MYSQL_DWH["url"])
            .option("dbtable",  f"jaklingko_dwh.{tabel}")
            .option("user",     MYSQL_DWH["user"])
            .option("password", MYSQL_DWH["password"])
            .option("driver",   MYSQL_DWH["driver"])
            .load())


def simpan(nama):
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"{nama}.png"),
                dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[VIZ] Tersimpan: {nama}.png")


# ============================================================
# QUERY 1 – Volume & Pendapatan per Hari (April 2023)
#            Roll-up: Tahun → Bulan → Hari
# ============================================================
def q1_tren_harian(spark):
    print("\n=== Q1: Tren Harian Volume & Pendapatan (April 2023) ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dw   = baca(spark, "dim_waktu")

    hasil = (
        fact.filter(col("is_sukses") == 1)
        .join(dw, "waktu_key")
        .groupBy("tanggal","nama_hari","is_weekend")
        .agg(
            count("transaksi_key").alias("jml_transaksi"),
            _sum("total_bayar").alias("total_pendapatan"),
            _round(avg("durasi_menit"), 1).alias("avg_durasi"),
        )
        .orderBy("tanggal")
    )
    hasil.show(30)

    # Contoh output:
    # +----------+---------+----------+-------------+------------------+-----------+
    # |tanggal   |nama_hari|is_weekend|jml_transaksi|total_pendapatan  |avg_durasi |
    # +----------+---------+----------+-------------+------------------+-----------+
    # |2023-04-01|Saturday |1         |981          |2625500           |36.4       |
    # |2023-04-03|Monday   |0         |1462         |4324000           |38.2       |
    # |2023-04-04|Tuesday  |0         |1389         |3990500           |37.8       |
    # +----------+---------+----------+-------------+------------------+-----------+

    pd_h = hasil.toPandas()
    pd_h["tanggal"] = pd_h["tanggal"].astype(str)

    fig, ax1 = plt.subplots(figsize=(14, 5))
    colors = [WARNA[2] if w else WARNA[0]
              for w in pd_h["is_weekend"].fillna(0).astype(int)]
    bars = ax1.bar(pd_h["tanggal"], pd_h["jml_transaksi"],
                   color=colors, alpha=0.8, label="Volume Transaksi")
    ax1.set_ylabel("Jumlah Transaksi")
    ax1.set_xlabel("Tanggal")

    ax2 = ax1.twinx()
    ax2.plot(pd_h["tanggal"], pd_h["total_pendapatan"] / 1e6,
             color=WARNA[3], marker="o", linewidth=2, label="Pendapatan (Juta Rp)")
    ax2.set_ylabel("Pendapatan (Juta Rp)", color=WARNA[3])

    ax1.set_xticks(range(len(pd_h)))
    ax1.set_xticklabels(pd_h["tanggal"], rotation=45, ha="right", fontsize=7)
    ax1.set_title("Tren Harian Volume & Pendapatan Transjakarta – April 2023",
                  fontweight="bold")

    from matplotlib.patches import Patch
    legend_els = [Patch(facecolor=WARNA[0], label="Hari Kerja"),
                  Patch(facecolor=WARNA[2], label="Weekend")]
    ax1.legend(handles=legend_els, loc="upper left")
    simpan("q1_tren_harian")
    return hasil


# ============================================================
# QUERY 2 – Volume per Jenis Koridor & Bank Kartu
#            Slice: hanya transaksi Sukses (bukan Gratis/Inkomplет)
# ============================================================
def q2_koridor_bank(spark):
    print("\n=== Q2: Volume per Jenis Koridor & Bank Kartu ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dr   = baca(spark, "dim_rute")

    hasil = (
        fact.filter(col("is_sukses") == 1)
        .join(dr, "rute_key")
        .groupBy("jenis_koridor", "bank_kartu")
        .agg(
            count("transaksi_key").alias("jml_transaksi"),
            _sum("total_bayar").alias("total_pendapatan"),
            _round(avg("durasi_menit"), 1).alias("avg_durasi"),
        )
        .orderBy(col("jml_transaksi").desc())
    )
    hasil.show(30)

    # Contoh output:
    # +------------------+-----------+-------------+------------------+-----------+
    # |jenis_koridor     |bank_kartu |jml_transaksi|total_pendapatan  |avg_durasi |
    # +------------------+-----------+-------------+------------------+-----------+
    # |BRT Utama         |dki        |6842         |23947000          |38.1       |
    # |Jak Lingko        |dki        |5211         |18238500          |42.3       |
    # |BRT Cabang        |emoney     |3104         |10864000          |35.7       |
    # |Transjabodetabek  |dki        |2890         |57800000          |55.2       |
    # |Royaltrans        |flazz      |1240         |24800000          |28.4       |
    # +------------------+-----------+-------------+------------------+-----------+

    pd_h = hasil.toPandas()
    pivot = pd_h.pivot_table(
        index="jenis_koridor", columns="bank_kartu",
        values="jml_transaksi", aggfunc="sum", fill_value=0
    )

    fig, ax = plt.subplots(figsize=(13, 5))
    pivot.sort_values(pivot.columns[0], ascending=False).plot(
        kind="bar", ax=ax, color=WARNA[:len(pivot.columns)],
        edgecolor="white"
    )
    ax.set_title("Volume Transaksi per Jenis Koridor & Bank Kartu\n"
                 "(Hanya Transaksi Sukses, April 2023)", fontweight="bold")
    ax.set_xlabel("Jenis Koridor")
    ax.set_ylabel("Jumlah Transaksi")
    ax.legend(title="Bank Kartu", bbox_to_anchor=(1, 1))
    ax.tick_params(axis="x", rotation=25)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    simpan("q2_koridor_bank")
    return hasil


# ============================================================
# QUERY 3 – Analisis Jam Sibuk per Arah Perjalanan
#            Dice: hari kerja + arah 0 (pergi) vs 1 (pulang)
# ============================================================
def q3_jam_arah(spark):
    print("\n=== Q3: Distribusi Jam per Arah Perjalanan (Hari Kerja) ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dw   = baca(spark, "dim_waktu")

    hasil = (
        fact.filter((col("is_sukses") == 1) |
                    (col("is_gratis") == 1))
        .join(dw.filter(col("is_weekend") == 0), "waktu_key")
        .groupBy("jam_tap_in", "sesi_hari", "arah_perjalanan")
        .agg(
            count("transaksi_key").alias("jml_transaksi"),
            _round(avg("total_bayar"), 0).alias("avg_bayar"),
        )
        .orderBy("jam_tap_in", "arah_perjalanan")
    )
    hasil.show(50)

    # Contoh output:
    # +-----------+----------+-----------------+-------------+----------+
    # |jam_tap_in |sesi_hari |arah_perjalanan  |jml_transaksi|avg_bayar |
    # +-----------+----------+-----------------+-------------+----------+
    # |6          |Pagi      |0                |1823         |3288      |
    # |6          |Pagi      |1                |412          |3500      |
    # |7          |Pagi      |0                |3941         |3412      |  ← PEAK PAGI PERGI
    # |8          |Pagi      |0                |3102         |3350      |
    # |17         |Sore      |1                |4120         |3488      |  ← PEAK SORE PULANG
    # |18         |Sore      |1                |3640         |3490      |
    # +-----------+----------+-----------------+-------------+----------+

    pd_h = hasil.toPandas()
    pergi  = pd_h[pd_h["arah_perjalanan"] == 0].set_index("jam_tap_in")["jml_transaksi"]
    pulang = pd_h[pd_h["arah_perjalanan"] == 1].set_index("jam_tap_in")["jml_transaksi"]

    fig, ax = plt.subplots(figsize=(12, 5))
    jams = list(range(24))
    ax.plot(jams, [pergi.get(j, 0) for j in jams],
            color=WARNA[0], marker="o", linewidth=2, label="Arah 0 (Pergi)")
    ax.plot(jams, [pulang.get(j, 0) for j in jams],
            color=WARNA[2], marker="s", linewidth=2, label="Arah 1 (Pulang)")
    ax.set_xticks(jams)
    ax.set_xticklabels([f"{h:02d}:00" for h in jams], rotation=45, ha="right")
    ax.set_ylabel("Jumlah Transaksi")
    ax.set_title("Distribusi Jam Sibuk per Arah Perjalanan – Hari Kerja",
                 fontweight="bold")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    simpan("q3_jam_arah")
    return hasil


# ============================================================
# QUERY 4 – Profil Pengguna: Segmen & Kelompok Usia vs Bank
#            Dice: filter hanya pengguna aktif (>= 1 transaksi sukses)
# ============================================================
def q4_profil_pengguna(spark):
    print("\n=== Q4: Distribusi Segmen Pengguna & Bank Kartu ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dp   = baca(spark, "dim_pengguna")

    hasil = (
        fact.filter(col("is_sukses") == 1)
        .join(dp, "pengguna_key")
        .groupBy("kelompok_usia", "segment_pengguna", "bank_kartu", "gender")
        .agg(
            count("transaksi_key").alias("jml_transaksi"),
            F.countDistinct("pengguna_id").alias("jml_pengguna"),
            _round(avg("total_bayar"), 0).alias("avg_bayar"),
        )
        .orderBy(col("jml_transaksi").desc())
    )
    hasil.show(20)

    # Contoh output:
    # +--------------+------------------+----------+------+-------------+-------------+----------+
    # |kelompok_usia |segment_pengguna  |bank_kartu|gender|jml_transaksi|jml_pengguna |avg_bayar |
    # +--------------+------------------+----------+------+-------------+-------------+----------+
    # |33-42 thn     |Pekerja           |dki       |F     |4120         |218          |3421      |
    # |43-52 thn     |Pekerja           |emoney    |M     |3640         |190          |3388      |
    # |23-32 thn     |Mahasiswa         |online    |F     |2410         |141          |1240      |
    # |>= 53 thn     |Lansia            |dki       |M     |2180         |108          |2890      |
    # |<= 22 thn     |Pelajar           |dki       |F     |1830         |95           |1050      |
    # +--------------+------------------+----------+------+-------------+-------------+----------+

    pd_h = hasil.toPandas()

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Kiri: Volume per kelompok usia & gender
    pivot_usia = pd_h.pivot_table(
        index="kelompok_usia", columns="gender",
        values="jml_transaksi", aggfunc="sum", fill_value=0
    )
    pivot_usia.plot(kind="bar", ax=axes[0],
                    color=[WARNA[0], WARNA[2]], edgecolor="white")
    axes[0].set_title("Volume per Kelompok Usia & Gender", fontweight="bold")
    axes[0].set_xlabel("Kelompok Usia")
    axes[0].set_ylabel("Jumlah Transaksi")
    axes[0].tick_params(axis="x", rotation=30)
    axes[0].legend(title="Gender")

    # Kanan: Pie distribusi bank kartu
    bank_dist = pd_h.groupby("bank_kartu")["jml_transaksi"].sum()
    axes[1].pie(bank_dist, labels=bank_dist.index,
                autopct="%1.1f%%", colors=WARNA[:len(bank_dist)],
                startangle=140)
    axes[1].set_title("Distribusi Bank Kartu Pengguna", fontweight="bold")

    simpan("q4_profil_pengguna")
    return hasil


# ============================================================
# QUERY 5 – Top 10 Halte Tersibuk + Persentase Tap-Out Hilang
#            Ranking + analisis kualitas data (is_inkomplet)
# ============================================================
def q5_top_halte_inkomplet(spark):
    print("\n=== Q5: Top 10 Halte Tersibuk + Inkomplет Rate ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dh   = baca(spark, "dim_halte")

    # Hitung per halte naik: total penumpang + persentase inkomplет
    hasil = (
        fact
        .join(dh.alias("hn"), col("halte_naik_key") == col("hn.halte_key"))
        .groupBy(col("hn.halte_id"), col("hn.nama_halte"))
        .agg(
            count("transaksi_key").alias("total_transaksi"),
            _sum(when(col("status_transaksi") == "Inkomplет", 1)
                 .otherwise(0)).alias("jml_inkomplet"),
            _sum(when(col("is_gratis") == 1, 1)
                 .otherwise(0)).alias("jml_gratis"),
            _round(avg("total_bayar"), 0).alias("avg_bayar"),
        )
        .withColumn("pct_inkomplet",
            _round(col("jml_inkomplet") / col("total_transaksi") * 100, 1))
        .withColumn("ranking",
            F.rank().over(Window.orderBy(col("total_transaksi").desc())))
        .filter(col("ranking") <= 10)
        .orderBy("ranking")
    )
    hasil.show(10)

    # Contoh output:
    # +--------+------------------------+---------------+--------------+-----------+----------+--------------+-------+
    # |halte_id|nama_halte              |total_transaksi|jml_inkomplet |jml_gratis |avg_bayar |pct_inkomplet |ranking|
    # +--------+------------------------+---------------+--------------+-----------+----------+--------------+-------+
    # |B00251P |Penjaringan 2           |312            |41            |28         |2890      |13.1          |1      |
    # |P00142  |Pal Putih               |298            |38            |22         |3180      |12.8          |2      |
    # |B01963P |Kemenkes 2              |276            |31            |18         |3420      |11.2          |3      |
    # +--------+------------------------+---------------+--------------+-----------+----------+--------------+-------+

    pd_h = hasil.toPandas().sort_values("total_transaksi")

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(pd_h["nama_halte"], pd_h["total_transaksi"],
            color=WARNA[1], alpha=0.8, label="Sukses + Gratis")
    ax.barh(pd_h["nama_halte"], pd_h["jml_inkomplet"],
            color=WARNA[2], alpha=0.8, label="Inkomplет (tidak tap-out)")

    # Anotasi pct inkomplet
    for i, row in pd_h.iterrows():
        ax.text(row["total_transaksi"] + 2,
                list(pd_h["nama_halte"]).index(row["nama_halte"]),
                f'{row["pct_inkomplet"]}% inkomplет',
                va="center", fontsize=8, color=WARNA[2])

    ax.set_title("Top 10 Halte Tersibuk + Inkomplет Rate\n"
                 "(Inkomplет = penumpang tidak tap-out)", fontweight="bold")
    ax.set_xlabel("Jumlah Transaksi")
    ax.legend()
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    simpan("q5_top_halte_inkomplet")
    return hasil


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    spark = buat_spark()
    q1_tren_harian(spark)
    q2_koridor_bank(spark)
    q3_jam_arah(spark)
    q4_profil_pengguna(spark)
    q5_top_halte_inkomplet(spark)
    print(f"\n[SELESAI] Grafik tersimpan di folder: {OUTPUT_DIR}/")
    spark.stop()
