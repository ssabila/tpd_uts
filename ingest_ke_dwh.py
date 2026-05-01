"""
============================================================
 JAKLINGKO DATA PIPELINE – UTS
 FILE  : ingest_ke_dwh.py
 DESC  : Ingest dfTransjakarta.csv (atau JSON/Excel format
         sama) LANGSUNG ke DWH MySQL (jaklingko_dwh),
         termasuk generate armada_id SINTETIS.

 FORMAT armada_id : ARM-{corridorID}-{direction}-{YYYYMMDD}
 Contoh           : ARM-5-1-20230403, ARM-R1A-0-20230403
 Logika           : 1 bus = 1 koridor + 1 arah + 1 hari
 Hasil            : ~10.826 armada unik, avg 3.5 penumpang

 Cara pakai:
   python ingest_ke_dwh.py dfTransjakarta.csv
   python ingest_ke_dwh.py data/ingest/

 Install:
   pip install pandas sqlalchemy pymysql openpyxl
============================================================
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, date
import logging, os, re, sys

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BATCH_ID  = datetime.now().strftime("INGEST_%Y%m%d_%H%M%S")
TAHUN_REF = 2023

# Referensi jenis & kapasitas kendaraan per jenis koridor
KAPASITAS_REF = {
    "BRT Utama"       : ("Bus Articulasi AC",  140),
    "BRT Cabang"      : ("Bus Reguler AC",      95),
    "Royaltrans"      : ("Bus Premium",         35),
    "Mikrotrans"      : ("Angkot/Van",          14),
    "Jak Lingko"      : ("Bus Sedang AC",       50),
    "Transjabodetabek": ("Bus AKDP AC",         55),
    "Lainnya"         : ("Bus Umum",            60),
}

# ─── Koneksi DWH ─────────────────────────────────────────
def engine_dwh():
    url = ("mysql+pymysql://root:Bintang123"
           "@localhost:3306/jaklingko_dwh?charset=utf8mb4")
    return create_engine(url, pool_pre_ping=True)


# ============================================================
# STEP 1 – BACA FILE (CSV / JSON / Excel)
# ============================================================
def baca_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    log.info(f"Membaca: {path}  (format: {ext})")
    if ext == ".csv":
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    elif ext == ".json":
        try:
            df = pd.read_json(path, orient="records")
        except ValueError:
            df = pd.read_json(path, lines=True)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
        df.columns = df.columns.str.strip().str.replace(" ", "")
    else:
        raise ValueError(f"Format tidak didukung: {ext}")
    log.info(f"  {len(df):,} baris dibaca")
    return df


# ============================================================
# STEP 2 – VALIDASI & CLEANING
# ============================================================
KOLOM_WAJIB = ["transID","payCardID","payCardBank","payCardName",
               "payCardSex","tapInStops","tapInStopsName",
               "tapInStopsLat","tapInStopsLon","tapInTime"]
BANK_VALID = {"dki","emoney","brizzi","flazz","online","bni"}

def validasi_dan_bersihkan(df: pd.DataFrame) -> pd.DataFrame:
    tidak_ada = [k for k in KOLOM_WAJIB if k not in df.columns]
    if tidak_ada:
        raise ValueError(f"Kolom wajib tidak ditemukan: {tidak_ada}")

    awal = len(df)
    for c in df.select_dtypes("object").columns:
        df[c] = df[c].astype(str).str.strip().replace("nan", np.nan)

    df = df.dropna(subset=["transID","payCardID","tapInStops","tapInTime"])
    df = df.drop_duplicates(subset=["transID"])

    df["payCardBank"] = df["payCardBank"].str.lower().str.strip()
    df.loc[~df["payCardBank"].isin(BANK_VALID), "payCardBank"] = "lainnya"

    df["payCardSex"] = df["payCardSex"].str.upper().str.strip()
    df.loc[~df["payCardSex"].isin(["M","F"]), "payCardSex"] = "Tidak Disebutkan"

    df["payCardBirthDate"] = pd.to_numeric(
        df["payCardBirthDate"], errors="coerce").astype("Int64")
    df.loc[(df["payCardBirthDate"] < 1940) |
           (df["payCardBirthDate"] > 2015), "payCardBirthDate"] = pd.NA

    df["tapInTime"]  = pd.to_datetime(df["tapInTime"],  errors="coerce")
    df["tapOutTime"] = pd.to_datetime(df.get("tapOutTime",
                                              pd.Series(dtype=str)), errors="coerce")
    df = df.dropna(subset=["tapInTime"])

    df["payAmount"]      = pd.to_numeric(df.get("payAmount", 0),   errors="coerce").fillna(0)
    df["tapInStopsLat"]  = pd.to_numeric(df["tapInStopsLat"],       errors="coerce")
    df["tapInStopsLon"]  = pd.to_numeric(df["tapInStopsLon"],       errors="coerce")
    df["tapOutStopsLat"] = pd.to_numeric(df.get("tapOutStopsLat"),  errors="coerce")
    df["tapOutStopsLon"] = pd.to_numeric(df.get("tapOutStopsLon"),  errors="coerce")
    df["stopStartSeq"]   = pd.to_numeric(df.get("stopStartSeq"),    errors="coerce").astype("Int64")
    df["stopEndSeq"]     = pd.to_numeric(df.get("stopEndSeq"),      errors="coerce").astype("Int64")
    df["direction"]      = pd.to_numeric(df.get("direction", 0),    errors="coerce").astype("Int64")

    df["corridorID"]      = df.get("corridorID",      pd.Series(dtype=str)).fillna("UNKNOWN")
    df["corridorName"]    = df.get("corridorName",    pd.Series(dtype=str)).fillna("Tidak Diketahui")
    df["tapOutStops"]     = df.get("tapOutStops",     pd.Series(dtype=str))
    df["tapOutStopsName"] = df.get("tapOutStopsName", pd.Series(dtype=str))

    log.info(f"  Validasi: {awal:,} → {len(df):,} baris ({awal-len(df):,} dibuang)")
    return df.reset_index(drop=True)


# ============================================================
# STEP 3 – KOLOM TURUNAN termasuk armada_id sintetis
# ============================================================

def _jenis_koridor(cid: str) -> str:
    if pd.isna(cid) or str(cid) in ("UNKNOWN",""):
        return "Lainnya"
    cid = str(cid).strip()
    if cid.startswith("JAK."): return "Jak Lingko"
    if re.match(r"^R\d", cid):  return "Royaltrans"
    if re.match(r"^M\d", cid):  return "Mikrotrans"
    if re.match(r"^\d+$", cid): return "BRT Utama"
    if re.match(r"^\d+[A-Za-z]", cid): return "BRT Cabang"
    return "Transjabodetabek"


def generate_armada_id(corridor_id, direction, tanggal) -> str:
    """
    Generate armada_id sintetis.

    Format : ARM-{corridorID}-{direction}-{YYYYMMDD}
    Contoh : ARM-5-1-20230403
             ARM-R1A-0-20230403
             ARM-JAK.18-0-20230403

    Logika : Tidak ada kolom kendaraan di dataset Kaggle.
             Diasumsikan 1 bus melayani 1 koridor + 1 arah
             dalam 1 hari penuh = 1 armada sintetis.
    """
    cid     = str(corridor_id).strip() if pd.notna(corridor_id) else "UNK"
    dir_str = str(int(direction))      if pd.notna(direction)    else "0"
    tgl_str = pd.Timestamp(tanggal).strftime("%Y%m%d") if tanggal else "00000000"
    return f"ARM-{cid}-{dir_str}-{tgl_str}"


def tambah_kolom_turunan(df: pd.DataFrame) -> pd.DataFrame:
    # Waktu
    df["tanggal_perjalanan"] = df["tapInTime"].dt.date
    df["jam_tap_in"]         = df["tapInTime"].dt.hour
    df["sesi_hari"] = pd.cut(df["jam_tap_in"],
                              bins=[-1,4,9,14,18,24],
                              labels=["Malam","Pagi","Siang","Sore","Malam2"]
                             ).astype(str).replace("Malam2","Malam")
    df["durasi_menit"] = (
        (df["tapOutTime"] - df["tapInTime"])
        .dt.total_seconds().div(60).round(0).astype("Int64")
    )

    # Status
    def status(row):
        if pd.isna(row["tapOutTime"]): return "Inkomplет"
        if row["payAmount"] == 0:      return "Gratis"
        return "Sukses"
    df["status_transaksi"] = df.apply(status, axis=1)
    df["is_sukses"] = (df["status_transaksi"] == "Sukses").astype(int)
    df["is_gratis"] = (df["status_transaksi"] == "Gratis").astype(int)

    # Pengguna
    df["usia"] = (TAHUN_REF - df["payCardBirthDate"]).astype("Int64")
    def kelompok(u):
        if pd.isna(u): return "Tidak Diketahui"
        if u <= 22: return "<= 22 thn"
        if u <= 32: return "23-32 thn"
        if u <= 42: return "33-42 thn"
        if u <= 52: return "43-52 thn"
        return ">= 53 thn"
    def segmen(u):
        if pd.isna(u): return "Tidak Diketahui"
        if u <= 17: return "Pelajar"
        if u <= 24: return "Mahasiswa"
        if u <= 55: return "Pekerja"
        return "Lansia"
    df["kelompok_usia"]    = df["usia"].apply(kelompok)
    df["segment_pengguna"] = df["usia"].apply(segmen)

    # Rute
    df["jenis_koridor"] = df["corridorID"].apply(_jenis_koridor)
    def split_asal(n):
        if pd.isna(n) or " - " not in str(n): return str(n) if pd.notna(n) else ""
        return str(n).split(" - ")[0].strip()
    def split_tujuan(n):
        if pd.isna(n) or " - " not in str(n): return ""
        p = str(n).split(" - ", 1)
        return p[1].strip() if len(p) > 1 else ""
    df["asal"]   = df["corridorName"].apply(split_asal)
    df["tujuan"] = df["corridorName"].apply(split_tujuan)

    # ── ARMADA SINTETIS ──────────────────────────────
    df["armada_id"] = df.apply(
        lambda r: generate_armada_id(r["corridorID"],
                                     r["direction"],
                                     r["tanggal_perjalanan"]),
        axis=1
    )
    n_armada = df["armada_id"].nunique()
    log.info(f"  armada_id sintetis: {n_armada:,} armada unik "
             f"(avg {len(df)/n_armada:.1f} penumpang/armada)")
    return df


# ============================================================
# STEP 4 – BANGUN TABEL DIMENSI
# ============================================================

def bangun_dim_waktu(df):
    tgl = pd.to_datetime(pd.Series(df["tanggal_perjalanan"].unique())).sort_values()
    dw  = pd.DataFrame({"tanggal": tgl})
    dw["tahun"]              = dw["tanggal"].dt.year.astype("int16")
    dw["kuartal"]            = dw["tanggal"].dt.quarter.astype("int8")
    dw["bulan"]              = dw["tanggal"].dt.month.astype("int8")
    dw["nama_bulan"]         = dw["tanggal"].dt.strftime("%B")
    dw["minggu_dalam_tahun"] = dw["tanggal"].dt.isocalendar().week.astype("int8")
    dw["hari_dalam_bulan"]   = dw["tanggal"].dt.day.astype("int8")
    dw["hari_dalam_minggu"]  = (dw["tanggal"].dt.dayofweek + 1).astype("int8")
    dw["nama_hari"]          = dw["tanggal"].dt.strftime("%A")
    dw["is_weekend"]         = dw["tanggal"].dt.dayofweek.isin([5,6]).astype(int)
    dw["is_hari_libur"]      = 0
    dw["keterangan_libur"]   = None
    dw["waktu_key"]          = range(1, len(dw)+1)
    log.info(f"  dim_waktu: {len(dw)} tanggal")
    return dw


def bangun_dim_halte(df):
    h_in  = df[["tapInStops","tapInStopsName","tapInStopsLat","tapInStopsLon"]
               ].dropna(subset=["tapInStops"]).copy()
    h_in.columns  = ["halte_id","nama_halte","latitude","longitude"]
    h_out = df[["tapOutStops","tapOutStopsName","tapOutStopsLat","tapOutStopsLon"]
               ].dropna(subset=["tapOutStops"]).copy()
    h_out.columns = ["halte_id","nama_halte","latitude","longitude"]
    dim_h = (pd.concat([h_in, h_out]).drop_duplicates("halte_id").reset_index(drop=True))
    dim_h["valid_dari"]   = date.today()
    dim_h["valid_sampai"] = None
    dim_h["is_current"]   = 1
    dim_h["halte_key"]    = range(1, len(dim_h)+1)
    log.info(f"  dim_halte: {len(dim_h):,} halte")
    return dim_h


def bangun_dim_rute(df):
    dim_r = (df[["corridorID","corridorName","jenis_koridor","asal","tujuan"]]
               .drop_duplicates("corridorID")
               .rename(columns={"corridorID":"rute_id","corridorName":"nama_rute"})
               .reset_index(drop=True))
    dim_r["valid_dari"]   = date.today()
    dim_r["valid_sampai"] = None
    dim_r["is_current"]   = 1
    dim_r["rute_key"]     = range(1, len(dim_r)+1)
    log.info(f"  dim_rute: {len(dim_r)} koridor")
    return dim_r


def bangun_dim_pengguna(df):
    dim_p = (df.sort_values("payCardBirthDate", na_position="last")
               .drop_duplicates("payCardID")
               [["payCardID","payCardName","payCardBank","payCardSex",
                 "payCardBirthDate","usia","kelompok_usia","segment_pengguna"]]
               .rename(columns={"payCardID":"pengguna_id","payCardName":"nama_lengkap",
                                "payCardBank":"bank_kartu","payCardSex":"gender",
                                "payCardBirthDate":"tahun_lahir"})
               .reset_index(drop=True))
    dim_p["valid_dari"]   = date.today()
    dim_p["valid_sampai"] = None
    dim_p["is_current"]   = 1
    dim_p["pengguna_key"] = range(1, len(dim_p)+1)
    log.info(f"  dim_pengguna: {len(dim_p):,} pengguna")
    return dim_p


def bangun_dim_armada(df):
    """
    Bangun dim_armada dari kombinasi unik armada_id sintetis.
    """
    dim_a = (df[["armada_id","corridorID","direction",
                 "tanggal_perjalanan","jenis_koridor"]]
               .drop_duplicates("armada_id")
               .rename(columns={"corridorID"       :"rute_id",
                                "direction"         :"arah_perjalanan",
                                "tanggal_perjalanan":"tanggal_operasi"})
               .reset_index(drop=True))

    dim_a["jenis_armada"] = dim_a["jenis_koridor"].map(
        lambda k: KAPASITAS_REF.get(k, KAPASITAS_REF["Lainnya"])[0])
    dim_a["kapasitas"] = dim_a["jenis_koridor"].map(
        lambda k: KAPASITAS_REF.get(k, KAPASITAS_REF["Lainnya"])[1])
    dim_a["is_sintetis"] = 1
    dim_a["catatan"]     = "ID sintetis Kaggle: ARM-corridorID-dir-YYYYMMDD"
    dim_a["armada_key"]  = range(1, len(dim_a)+1)
    dim_a["arah_perjalanan"] = dim_a["arah_perjalanan"].astype("Int64")

    # ─── PERBAIKAN: Hapus kolom yang tidak ada di skema SQL DWH ───
    dim_a = dim_a.drop(columns=["jenis_koridor"]) # <--- Tambahkan baris ini
    
    log.info(f"  dim_armada: {len(dim_a):,} armada sintetis "
             f"(avg {len(df)/len(dim_a):.1f} penumpang/armada)")
    return dim_a


# ============================================================
# STEP 5 – BANGUN FACT TABLE
# ============================================================

def bangun_fact(df, dim_waktu, dim_halte, dim_rute,
                dim_pengguna, dim_armada, sumber):
    map_w  = dict(zip(pd.to_datetime(dim_waktu["tanggal"]).dt.date, dim_waktu["waktu_key"]))
    map_h  = dict(zip(dim_halte["halte_id"],    dim_halte["halte_key"]))
    map_r  = dict(zip(dim_rute["rute_id"],      dim_rute["rute_key"]))
    map_p  = dict(zip(dim_pengguna["pengguna_id"], dim_pengguna["pengguna_key"]))
    map_bk = dict(zip(dim_pengguna["pengguna_id"], dim_pengguna["bank_kartu"]))
    map_a  = dict(zip(dim_armada["armada_id"],  dim_armada["armada_key"]))

    df["waktu_key"]       = df["tanggal_perjalanan"].map(map_w)
    df["halte_naik_key"]  = df["tapInStops"].map(map_h)
    df["halte_turun_key"] = df["tapOutStops"].map(map_h)
    df["rute_key"]        = df["corridorID"].map(map_r)
    df["pengguna_key"]    = df["payCardID"].map(map_p)
    df["bank_kartu"]      = df["payCardID"].map(map_bk)
    df["armada_key"]      = df["armada_id"].map(map_a)   # ← FK ke dim_armada

    sebelum = len(df)
    df = df.dropna(subset=["waktu_key","halte_naik_key","pengguna_key"])
    if len(df) < sebelum:
        log.warning(f"  {sebelum-len(df):,} baris dibuang (gagal lookup)")

    fact = pd.DataFrame({
        "waktu_key"       : df["waktu_key"].astype("Int64"),
        "halte_naik_key"  : df["halte_naik_key"].astype("Int64"),
        "halte_turun_key" : df["halte_turun_key"].astype("Int64"),
        "rute_key"        : df["rute_key"].astype("Int64"),
        "pengguna_key"    : df["pengguna_key"].astype("Int64"),
        "armada_key"      : df["armada_key"].astype("Int64"),   
        "feedback_kategori_key": -1,  # ← TAMBAHAN: Set ke -1 (Unknown) karena CSV tidak ada feedback
        
        "transaksi_id"    : df["transID"].astype(str),
        "armada_id"       : df["armada_id"].astype(str),        
        
        "waktu_tap_in"    : df["tapInTime"],
        "waktu_tap_out"   : df["tapOutTime"],
        "jam_tap_in"      : df["jam_tap_in"].astype("Int8"),
        "sesi_hari"       : df["sesi_hari"].astype(str),
        
        # Measures
        "total_bayar"     : df["payAmount"].astype(float),
        "durasi_menit"    : df["durasi_menit"].astype("Int64"),
        
        # Kolom kosong untuk menyesuaikan dengan PySpark
        "tarif"           : None, # ← TAMBAHAN: Kosong untuk CSV
        "diskon"          : None, # ← TAMBAHAN: Kosong untuk CSV
        "jarak_tempuh_km" : None, # ← TAMBAHAN: Kosong untuk CSV
        
        "arah_perjalanan" : df["direction"].astype("Int64"),
        "urutan_naik"     : df["stopStartSeq"].astype("Int64"),
        "urutan_turun"    : df["stopEndSeq"].astype("Int64"),
        "bank_kartu"      : df["bank_kartu"].astype(str),
        
        "metode_bayar"    : None, # ← TAMBAHAN: Kosong untuk CSV
        "rating"          : None, # ← TAMBAHAN: Kosong untuk CSV
        "komentar"        : None, # ← TAMBAHAN: Kosong untuk CSV
        "has_feedback"    : 0,    # ← TAMBAHAN: Default 0
        
        "status_transaksi": df["status_transaksi"].astype(str),
        "is_sukses"       : df["is_sukses"].astype(int),
        "is_gratis"       : df["is_gratis"].astype(int),
        "sumber_data"     : sumber,
        "etl_batch_id"    : BATCH_ID,
    })
    log.info(f"  Fact: {len(fact):,} baris")
    return fact


# ============================================================
# STEP 6 – CEK DUPLIKAT & LOAD
# ============================================================

def cek_duplikat_dwh(df_fact, engine):
    ids = df_fact["transaksi_id"].tolist()
    if not ids: return df_fact
    existing = set()
    with engine.connect() as conn:
        for i in range(0, len(ids), 500):
            chunk = tuple(ids[i:i+500])
            res = conn.execute(
                text("SELECT transaksi_id FROM fact_transaksi_perjalanan "
                     "WHERE transaksi_id IN :ids"), {"ids": chunk})
            existing.update(r[0] for r in res)
    df_baru = df_fact[~df_fact["transaksi_id"].isin(existing)]
    if len(df_baru) < len(df_fact):
        log.info(f"  {len(df_fact)-len(df_baru):,} transaksi sudah ada, di-skip")
    return df_baru


def upsert_dim(df_dim, engine, tabel, pk_col):
    with engine.connect() as conn:
        ada = pd.read_sql(f"SELECT {pk_col} FROM {tabel}", conn)
    df_baru = df_dim[~df_dim[pk_col].isin(ada[pk_col])]
    if df_baru.empty:
        log.info(f"  {tabel}: tidak ada data baru")
        return
    df_baru.to_sql(tabel, engine, if_exists="append",
                   index=False, chunksize=500, method="multi")
    log.info(f"  ✓ {tabel}: +{len(df_baru):,} baris")


# Tambahkan df_mentah di baris ini
def muat_ke_dwh(dim_waktu, dim_halte, dim_rute, dim_pengguna,
                dim_armada, df_fact, engine, df_mentah):
    log.info("Memuat ke DWH (jaklingko_dwh)...")
    
    # ⚠️ PERBAIKAN: Hapus kolom _key agar tidak bentrok dengan AUTO_INCREMENT di MySQL
    # Kita hanya mengirim Business ID (seperti halte_id), MySQL akan kasih _key otomatis.
    
    upsert_dim(dim_waktu.drop(columns=['waktu_key']),       engine, "dim_waktu",    "tanggal")
    upsert_dim(dim_halte.drop(columns=['halte_key']),       engine, "dim_halte",    "halte_id")
    upsert_dim(dim_rute.drop(columns=['rute_key']),         engine, "dim_rute",     "rute_id")
    upsert_dim(dim_pengguna.drop(columns=['pengguna_key']), engine, "dim_pengguna", "pengguna_id")
    upsert_dim(dim_armada.drop(columns=['armada_key']),     engine, "dim_armada",   "armada_id")

    if df_fact.empty:
        log.info("  Tidak ada fact baru.")
        return

    # Untuk tabel fakta, kita harus mengambil kembali _key yang benar dari MySQL 
    # karena kita baru saja mendrop _key yang lama di atas.
    log.info("  Menyinkronkan foreign keys fakta dengan DWH...")
    with engine.connect() as conn:
        map_h = pd.read_sql("SELECT halte_id, halte_key FROM dim_halte", conn).set_index('halte_id')['halte_key'].to_dict()
        map_r = pd.read_sql("SELECT rute_id, rute_key FROM dim_rute", conn).set_index('rute_id')['rute_key'].to_dict()
        map_p = pd.read_sql("SELECT pengguna_id, pengguna_key FROM dim_pengguna", conn).set_index('pengguna_id')['pengguna_key'].to_dict()
        map_a = pd.read_sql("SELECT armada_id, armada_key FROM dim_armada", conn).set_index('armada_id')['armada_key'].to_dict()
        map_w = pd.read_sql("SELECT tanggal, waktu_key FROM dim_waktu", conn)
        map_w['tanggal'] = pd.to_datetime(map_w['tanggal']).dt.date
        map_w = map_w.set_index('tanggal')['waktu_key'].to_dict()

    # Update FK di df_fact dengan key asli dari database
    # (Ini menjamin integritas data tetap terjaga)
    # Gunakan df_mentah di sini
    df_fact['halte_naik_key']  = df_fact['transaksi_id'].map(dict(zip(df_mentah['transID'], df_mentah['tapInStops']))).map(map_h)
    df_fact['halte_turun_key'] = df_fact['transaksi_id'].map(dict(zip(df_mentah['transID'], df_mentah['tapOutStops']))).map(map_h)
    df_fact['rute_key']        = df_fact['transaksi_id'].map(dict(zip(df_mentah['transID'], df_mentah['corridorID']))).map(map_r)
    df_fact['pengguna_key']    = df_fact['transaksi_id'].map(dict(zip(df_mentah['transID'], df_mentah['payCardID']))).map(map_p)
    df_fact['armada_key']      = df_fact['armada_id'].map(map_a)
    df_fact['waktu_key']       = pd.to_datetime(df_fact['waktu_tap_in']).dt.date.map(map_w)

    # Isi yang masih kosong dengan -1 (Unknown)
    for col_key in ['halte_turun_key', 'rute_key', 'armada_key']:
        df_fact[col_key] = df_fact[col_key].fillna(-1).astype(int)

    try:
        df_fact.to_sql("fact_transaksi_perjalanan", engine,
                       if_exists="append", index=False,
                       chunksize=500, method="multi")
        log.info(f"  ✓ fact_transaksi_perjalanan: +{len(df_fact):,} baris")
    except SQLAlchemyError as e:
        log.error(f"  ✗ Gagal: {e}"); raise


# ============================================================
# FUNGSI UTAMA
# ============================================================

def ingest_ke_dwh(path_file: str):
    ext    = os.path.splitext(path_file)[1].lower()
    sumber = {".csv":"csv",".json":"json",".xlsx":"excel",".xls":"excel"}.get(ext,"file")

    log.info("=" * 60)
    log.info(f" INGEST KE DWH  | {os.path.basename(path_file)}")
    log.info(f" Batch ID       : {BATCH_ID}")
    log.info("=" * 60)

    engine = engine_dwh()
    df     = baca_file(path_file)
    df     = validasi_dan_bersihkan(df)
    df     = tambah_kolom_turunan(df)          # ← generate armada_id sintetis

    dim_waktu    = bangun_dim_waktu(df)
    dim_halte    = bangun_dim_halte(df)
    dim_rute     = bangun_dim_rute(df)
    dim_pengguna = bangun_dim_pengguna(df)
    dim_armada   = bangun_dim_armada(df)       # ← bangun dim_armada

    df_fact = bangun_fact(df, dim_waktu, dim_halte, dim_rute,
                          dim_pengguna, dim_armada, sumber)
    df_fact = cek_duplikat_dwh(df_fact, engine)
    muat_ke_dwh(dim_waktu, dim_halte, dim_rute, dim_pengguna,
                dim_armada, df_fact, engine, df)
    engine.dispose()

    log.info(f" SELESAI: {len(df_fact):,} transaksi dimuat ke DWH")
    log.info("=" * 60)


def ingest_folder(folder, ekstensi=(".csv",".json",".xlsx")):
    files = sorted([os.path.join(folder,f) for f in os.listdir(folder)
                    if f.lower().endswith(ekstensi)])
    if not files:
        log.info(f"Tidak ada file ditemukan di: {folder}"); return
    ok, gagal = 0, 0
    for f in files:
        try:   ingest_ke_dwh(f); ok += 1
        except Exception as e:
            log.error(f"  ✗ {f}: {e}"); gagal += 1
    log.info(f"Batch selesai: {ok} berhasil, {gagal} gagal")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("\nCara pakai:\n"
              "  python ingest_ke_dwh.py dfTransjakarta.csv\n"
              "  python ingest_ke_dwh.py data/ingest/\n")
        sys.exit(1)
    target = sys.argv[1]
    if   os.path.isdir(target):  ingest_folder(target)
    elif os.path.isfile(target): ingest_ke_dwh(target)
    else:
        log.error(f"Path tidak ditemukan: {target}"); sys.exit(1)
