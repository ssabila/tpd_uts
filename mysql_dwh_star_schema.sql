-- ============================================================
--  JAKLINGKO DATA PIPELINE – UTS
--  FILE    : mysql_dwh_star_schema.sql
--  DATABASE: jaklingko_dwh  (Target DWH – MySQL Workbench)
--  DATASET : dfTransjakarta.csv (Kaggle, April 2023)
--
--  Perubahan dari versi sebelumnya:
--    + dim_armada  : ditambahkan kembali (ID sintetis)
--    + fact        : armada_key FK ke dim_armada (nullable)
-- ============================================================

CREATE DATABASE IF NOT EXISTS jaklingko_dwh
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE jaklingko_dwh;

-- ============================================================
-- DIMENSI 1: dim_waktu
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_waktu (
    waktu_key            INT         NOT NULL AUTO_INCREMENT,
    tanggal              DATE        NOT NULL,
    tahun                SMALLINT    NOT NULL,
    kuartal              TINYINT     NOT NULL,
    bulan                TINYINT     NOT NULL,
    nama_bulan           VARCHAR(15) NOT NULL,
    minggu_dalam_tahun   TINYINT     NOT NULL,
    hari_dalam_bulan     TINYINT     NOT NULL,
    hari_dalam_minggu    TINYINT     NOT NULL COMMENT '1=Senin 7=Minggu',
    nama_hari            VARCHAR(15) NOT NULL,
    is_weekend           TINYINT(1)  NOT NULL DEFAULT 0,
    is_hari_libur        TINYINT(1)  NOT NULL DEFAULT 0,
    keterangan_libur     VARCHAR(100),
    PRIMARY KEY (waktu_key),
    UNIQUE KEY uq_tanggal (tanggal),
    INDEX idx_tahun_bulan (tahun, bulan)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- DIMENSI 2: dim_halte
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_halte (
    halte_key     INT           NOT NULL AUTO_INCREMENT,
    halte_id      VARCHAR(20)   NOT NULL COMMENT 'NK: P00142, B01963P',
    nama_halte    VARCHAR(150)  NOT NULL,
    latitude      DECIMAL(10,7) NOT NULL,
    longitude     DECIMAL(10,7) NOT NULL,
    valid_dari    DATE          NOT NULL,
    valid_sampai  DATE,
    is_current    TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (halte_key),
    INDEX idx_halte_id  (halte_id),
    INDEX idx_is_current(is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- DIMENSI 3: dim_rute
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_rute (
    rute_key      INT           NOT NULL AUTO_INCREMENT,
    rute_id       VARCHAR(20)   NOT NULL COMMENT 'NK: corridorID',
    nama_rute     VARCHAR(200)  NOT NULL,
    asal          VARCHAR(100),
    tujuan        VARCHAR(150),
    jenis_koridor VARCHAR(30)   NOT NULL,
    valid_dari    DATE          NOT NULL,
    valid_sampai  DATE,
    is_current    TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (rute_key),
    INDEX idx_rute_id      (rute_id),
    INDEX idx_jenis_koridor(jenis_koridor),
    INDEX idx_is_current   (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- DIMENSI 4: dim_pengguna
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_pengguna (
    pengguna_key     INT           NOT NULL AUTO_INCREMENT,
    pengguna_id      BIGINT        NOT NULL COMMENT 'NK: payCardID',
    nama_lengkap     VARCHAR(150)  NOT NULL,
    bank_kartu       VARCHAR(20)   NOT NULL COMMENT 'dki/emoney/brizzi/flazz/online/bni',
    gender           VARCHAR(5)    NOT NULL COMMENT 'M / F',
    tahun_lahir      SMALLINT,
    usia             TINYINT,
    kelompok_usia    VARCHAR(20)   NOT NULL,
    segment_pengguna VARCHAR(20)   NOT NULL,
    valid_dari       DATE          NOT NULL,
    valid_sampai     DATE,
    is_current       TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (pengguna_key),
    INDEX idx_pengguna_id(pengguna_id),
    INDEX idx_bank_kartu (bank_kartu),
    INDEX idx_is_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- DIMENSI 5: dim_armada  ← BARU (sintetis dari Kaggle)
--
-- armada_id format: ARM-{corridorID}-{direction}-{YYYYMMDD}
-- Contoh         : ARM-5-1-20230403
--
-- 1 baris = 1 armada (proxy trip bus harian)
-- Atribut jenis_armada & kapasitas diisi berdasarkan
-- jenis_koridor dari rute yang dilayani.
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_armada (
    armada_key       INT           NOT NULL AUTO_INCREMENT,
    armada_id        VARCHAR(40)   NOT NULL COMMENT 'NK sintetis: ARM-corridorID-dir-YYYYMMDD',
    rute_id          VARCHAR(20)   NOT NULL COMMENT 'corridorID yang dilayani',
    arah_perjalanan  TINYINT       NOT NULL COMMENT '0=pergi / 1=pulang',
    tanggal_operasi  DATE          NOT NULL COMMENT 'Tanggal armada beroperasi',
    jenis_armada     VARCHAR(60)   NOT NULL COMMENT 'Estimasi dari jenis koridor',
    kapasitas        SMALLINT      NOT NULL COMMENT 'Estimasi kapasitas',
    is_sintetis      TINYINT(1)    NOT NULL DEFAULT 1
                     COMMENT '1 = ID sintetis dari Kaggle, 0 = data nyata',
    catatan          VARCHAR(200),
    PRIMARY KEY (armada_key),
    UNIQUE KEY uq_armada_id (armada_id),
    INDEX idx_rute_id       (rute_id),
    INDEX idx_tgl_operasi   (tanggal_operasi),
    INDEX idx_arah          (arah_perjalanan)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimensi armada – sintetis dari kombinasi koridor+arah+tanggal Kaggle';


-- ============================================================
-- DIMENSI 5 : dim_feedback_kategori
-- Daftar kategori feedback untuk dimensionalitas
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_feedback_kategori (
    feedback_kategori_key  INT          NOT NULL AUTO_INCREMENT,
    kategori_nama          VARCHAR(50)  NOT NULL UNIQUE,
    deskripsi              VARCHAR(255),
    PRIMARY KEY (feedback_kategori_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimensi kategori feedback';
  
-- ============================================================
-- FACT: fact_transaksi_perjalanan
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_transaksi_perjalanan (
    transaksi_key    BIGINT        NOT NULL AUTO_INCREMENT,

    -- Foreign Keys ke Dimensi
    waktu_key        INT           NOT NULL,
    halte_naik_key   INT           NOT NULL,
    halte_turun_key  INT,
    rute_key         INT,
    pengguna_key     INT           NOT NULL,
    armada_key       INT           COMMENT 'FK ke dim_armada (sintetis)',

    -- Degenerate Dimension
    transaksi_id     VARCHAR(20)   NOT NULL COMMENT 'transID dari Kaggle',
    armada_id        VARCHAR(40)   COMMENT 'ARM-{corridorID}-{dir}-{YYYYMMDD}',

    -- Waktu detail
    waktu_tap_in     DATETIME      NOT NULL,
    waktu_tap_out    DATETIME,
    jam_tap_in       TINYINT       NOT NULL,
    sesi_hari        VARCHAR(10)   NOT NULL,

    -- Measures
    total_bayar      DECIMAL(10,2) NOT NULL,
    durasi_menit     SMALLINT,

    -- Atribut perjalanan dari Kaggle
    arah_perjalanan  TINYINT       COMMENT '0=pergi / 1=pulang',
    urutan_naik      SMALLINT      COMMENT 'stopStartSeq',
    urutan_turun     SMALLINT      COMMENT 'stopEndSeq',
    bank_kartu       VARCHAR(20)   NOT NULL COMMENT 'payCardBank proxy metode bayar',

    -- Status
    status_transaksi VARCHAR(15)   NOT NULL COMMENT 'Sukses/Inkomplет/Gratis',
    is_sukses        TINYINT(1)    NOT NULL DEFAULT 0,
    is_gratis        TINYINT(1)    NOT NULL DEFAULT 0,

    -- Metadata ETL
    sumber_data      VARCHAR(20)   NOT NULL COMMENT 'csv/json/excel/postgresql',
    etl_batch_id     VARCHAR(60)   NOT NULL,
    etl_loaded_at    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (transaksi_key),
    UNIQUE KEY uq_transaksi_id (transaksi_id),
    FOREIGN KEY (waktu_key)     REFERENCES dim_waktu(waktu_key),
    FOREIGN KEY (halte_naik_key)REFERENCES dim_halte(halte_key),
    FOREIGN KEY (pengguna_key)  REFERENCES dim_pengguna(pengguna_key),
    FOREIGN KEY (armada_key)    REFERENCES dim_armada(armada_key),
    INDEX idx_waktu_key    (waktu_key),
    INDEX idx_rute_key     (rute_key),
    INDEX idx_pengguna_key (pengguna_key),
    INDEX idx_armada_key   (armada_key),
    INDEX idx_armada_id    (armada_id),
    INDEX idx_tap_in       (waktu_tap_in),
    INDEX idx_sumber       (sumber_data)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- LOG AUDIT ETL
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_log (
    log_id       INT          NOT NULL AUTO_INCREMENT,
    batch_id     VARCHAR(60)  NOT NULL,
    nama_proses  VARCHAR(100) NOT NULL,
    sumber       VARCHAR(50)  NOT NULL,
    tabel_target VARCHAR(60)  NOT NULL,
    jumlah_baris INT          NOT NULL DEFAULT 0,
    status       ENUM('Sukses','Gagal','Partial') NOT NULL,
    pesan_error  TEXT,
    mulai_at     DATETIME     NOT NULL,
    selesai_at   DATETIME,
    PRIMARY KEY (log_id),
    INDEX idx_batch_id (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE fact_transaksi_perjalanan
    -- 1. Tambahkan kolom FK
    ADD COLUMN feedback_kategori_key INT COMMENT 'FK ke dim_feedback_kategori (PySpark)',
    
    -- 2. Tambahkan Measures & Metrik dari PySpark
    ADD COLUMN tarif DECIMAL(10,2) COMMENT 'Dari PySpark',
    ADD COLUMN diskon DECIMAL(10,2) COMMENT 'Dari PySpark',
    ADD COLUMN jarak_tempuh_km DECIMAL(5,2) COMMENT 'Dari PySpark',
    ADD COLUMN metode_bayar VARCHAR(20) COMMENT 'Dari PySpark',
    
    -- 3. Tambahkan kolom detail Feedback
    ADD COLUMN rating TINYINT COMMENT '1-5 bintang',
    ADD COLUMN komentar TEXT,
    ADD COLUMN has_feedback TINYINT(1) NOT NULL DEFAULT 0,
    
    -- 4. Tambahkan Index dan Foreign Key Constraint
    ADD INDEX idx_feedback_key (feedback_kategori_key),
    ADD CONSTRAINT fk_fact_feedback 
        FOREIGN KEY (feedback_kategori_key) 
        REFERENCES dim_feedback_kategori(feedback_kategori_key);

-- ============================================================
-- INITIAL DATA: UNKNOWN DIMENSIONS (-1)
-- ============================================================
INSERT IGNORE INTO dim_waktu (waktu_key, tanggal, tahun, kuartal, bulan, nama_bulan, minggu_dalam_tahun, hari_dalam_bulan, hari_dalam_minggu, nama_hari) 
VALUES (-1, '1900-01-01', 1900, 1, 1, 'Unknown', 1, 1, 1, 'Unknown');

INSERT IGNORE INTO dim_halte (halte_key, halte_id, nama_halte, latitude, longitude, valid_dari) 
VALUES (-1, 'UNKNOWN', 'Halte Tidak Diketahui', 0, 0, '1900-01-01');

INSERT IGNORE INTO dim_rute (rute_key, rute_id, nama_rute, jenis_koridor, valid_dari) 
VALUES (-1, 'UNKNOWN', 'Rute Tidak Diketahui', 'Unknown', '1900-01-01');

INSERT IGNORE INTO dim_pengguna (pengguna_key, pengguna_id, nama_lengkap, bank_kartu, gender, kelompok_usia, segment_pengguna, valid_dari) 
VALUES (-1, -1, 'Tidak Diketahui', 'Unknown', 'U', 'Unknown', 'Unknown', '1900-01-01');

INSERT IGNORE INTO dim_armada (armada_key, armada_id, rute_id, arah_perjalanan, tanggal_operasi, jenis_armada, kapasitas) 
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 0, '1900-01-01', 'Unknown', 0);

INSERT IGNORE INTO dim_feedback_kategori (feedback_kategori_key, kategori_nama, deskripsi)
VALUES (-1, 'Unknown', 'Tidak ada feedback');

SELECT * FROM fact_transaksi_perjalanan;

use jaklingko_dwh;
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE fact_transaksi_perjalanan;
TRUNCATE dim_armada;
TRUNCATE dim_halte;
TRUNCATE dim_rute;
TRUNCATE dim_pengguna;
SET FOREIGN_KEY_CHECKS=1;