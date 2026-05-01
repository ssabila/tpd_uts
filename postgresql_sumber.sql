-- ============================================================
--  JAKLINGKO DATA PIPELINE – UTS
--  FILE    : postgresql_sumber.sql
--  DATABASE: jaklingko_pg  (Sumber 2 – PostgreSQL)
--  DATASET : dfTransjakarta.csv (Kaggle, April 2023)
--
--  Penyesuaian dari dataset asli:
--    • pengguna → dari payCardID, payCardBank, payCardName,
--                      payCardSex, payCardBirthDate
--    • transaksi→ dari transID, corridorID, tapInStops,
--                      tapOutStops, tapInTime, tapOutTime,
--                      payAmount, direction
--    • payCardBank menggantikan metode_bayar (tidak ada di Kaggle)
--    • payCardBirthDate hanya berisi TAHUN (bukan tanggal lengkap)
-- ============================================================

-- Jalankan sebagai superuser:
-- CREATE DATABASE jaklingko_pg;
-- \c jaklingko_pg

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enum disesuaikan dengan nilai di dataset Kaggle
CREATE TYPE bank_kartu_enum AS ENUM
  ('dki','emoney','brizzi','flazz','online','bni','lainnya');

CREATE TYPE gender_enum AS ENUM
  ('M','F','Tidak Disebutkan');

CREATE TYPE status_trx_enum AS ENUM
  ('Sukses','Inkomplет','Gratis');

-- ------------------------------------------------------------
-- Tabel 1: pengguna
-- Sumber: payCardID (sebagai ID), payCardBank, payCardName,
--         payCardSex, payCardBirthDate
-- CATATAN: 1 payCardID bisa muncul berkali-kali di transaksi
--          (rata-rata 18,9 transaksi per pengguna)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pengguna (
    pengguna_id    BIGINT        NOT NULL,
    nama_lengkap   VARCHAR(150)  NOT NULL,
    bank_kartu     bank_kartu_enum NOT NULL,
    gender         gender_enum   NOT NULL DEFAULT 'Tidak Disebutkan',
    tahun_lahir    SMALLINT,
    tanggal_daftar DATE          NOT NULL DEFAULT CURRENT_DATE,
    is_aktif       BOOLEAN       NOT NULL DEFAULT TRUE,
    PRIMARY KEY (pengguna_id)
);

COMMENT ON COLUMN pengguna.pengguna_id IS 'payCardID dari Kaggle';
COMMENT ON COLUMN pengguna.nama_lengkap IS 'payCardName';
COMMENT ON COLUMN pengguna.bank_kartu IS 'payCardBank: dki/emoney/brizzi/flazz/online/bni';
COMMENT ON COLUMN pengguna.gender IS 'payCardSex: M/F';
COMMENT ON COLUMN pengguna.tahun_lahir IS 'payCardBirthDate (hanya tahun, ex: 1997)';

-- NOTE: PostgreSQL pakai BIGINT bukan UUID karena payCardID berupa angka besar

CREATE INDEX idx_pengguna_bank ON pengguna(bank_kartu);
CREATE INDEX idx_pengguna_gender ON pengguna(gender);

-- ------------------------------------------------------------
-- Tabel 2: transaksi
-- Sumber: transID, payCardID, corridorID,
--         tapInStops, tapOutStops,
--         tapInTime, tapOutTime,
--         payAmount, direction
--
-- Penyesuaian:
--   - transID         → transaksi_id (VARCHAR, bukan UUID)
--   - payAmount = 0   → status 'Gratis'
--   - tapOutTime null → status 'Inkomplет' (tidak tap-out)
--   - direction 0/1   → arah perjalanan (pergi/pulang)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaksi (
    transaksi_id      VARCHAR(20)       NOT NULL,
    pengguna_id       BIGINT            NOT NULL,
    rute_id           VARCHAR(20),
    halte_naik_id     VARCHAR(20),
    halte_turun_id    VARCHAR(20),
    arah_perjalanan   SMALLINT,
    urutan_naik       SMALLINT,
    urutan_turun      SMALLINT,
    waktu_tap_in      TIMESTAMPTZ       NOT NULL,
    waktu_tap_out     TIMESTAMPTZ,
    durasi_menit      SMALLINT
        GENERATED ALWAYS AS (
            CASE
                WHEN waktu_tap_out IS NOT NULL
                THEN EXTRACT(EPOCH FROM (waktu_tap_out - waktu_tap_in))::INT / 60
                ELSE NULL
            END
        ) STORED,
    total_bayar       NUMERIC(10,2)     NOT NULL,
    status            status_trx_enum   NOT NULL DEFAULT 'Sukses',
    created_at        TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaksi_id),
    FOREIGN KEY (pengguna_id) REFERENCES pengguna(pengguna_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

COMMENT ON COLUMN transaksi.transaksi_id IS 'transID dari Kaggle';
COMMENT ON COLUMN transaksi.pengguna_id IS 'payCardID';
COMMENT ON COLUMN transaksi.rute_id IS 'corridorID';
COMMENT ON COLUMN transaksi.halte_naik_id IS 'tapInStops';
COMMENT ON COLUMN transaksi.halte_turun_id IS 'tapOutStops';
COMMENT ON COLUMN transaksi.arah_perjalanan IS 'direction: 0=pergi 1=pulang';
COMMENT ON COLUMN transaksi.urutan_naik IS 'stopStartSeq';
COMMENT ON COLUMN transaksi.urutan_turun IS 'stopEndSeq';
COMMENT ON COLUMN transaksi.waktu_tap_in IS 'tapInTime';
COMMENT ON COLUMN transaksi.waktu_tap_out IS 'tapOutTime (nullable = tidak tap-out)';
COMMENT ON COLUMN transaksi.total_bayar IS 'payAmount: 0.0 / 3500.0 / 20000.0';

CREATE INDEX idx_trx_pengguna  ON transaksi(pengguna_id);
CREATE INDEX idx_trx_rute      ON transaksi(rute_id);
CREATE INDEX idx_trx_waktu     ON transaksi(waktu_tap_in);
CREATE INDEX idx_trx_status    ON transaksi(status);
CREATE INDEX idx_trx_halte_in  ON transaksi(halte_naik_id);

-- ============================================================
-- CATATAN STATUS TRANSAKSI (dari analisis data Kaggle):
--   payAmount = 3500  → Sukses (tarif normal BRT)
--   payAmount = 20000 → Sukses (tarif Royaltrans/premium)
--   payAmount = 0     → Gratis (bebas bayar)
--   tapOutTime NULL   → Inkomplет (tidak tap-out)
-- ============================================================

-- ------------------------------------------------------------
-- Tabel 3: feedback (Sudah Diperbaiki)
-- ------------------------------------------------------------
CREATE TYPE kategori_feedback_enum AS ENUM
  ('Umum','Fasilitas Busway','Aplikasi Jaklingko');

CREATE TABLE IF NOT EXISTS feedback (
    feedback_id    UUID                      NOT NULL DEFAULT uuid_generate_v4(),
    
    -- [PERBAIKAN] Disamakan dengan PK di tabel transaksi
    transaksi_id   VARCHAR(20)               NOT NULL UNIQUE, 
    
    -- [PERBAIKAN] Disamakan dengan PK di tabel pengguna
    pengguna_id    BIGINT                    NOT NULL,        
    
    rating         SMALLINT                  NOT NULL CHECK (rating BETWEEN 1 AND 5),
    komentar       TEXT,
    kategori       kategori_feedback_enum    NOT NULL DEFAULT 'Umum',
    created_at     TIMESTAMPTZ               NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (feedback_id),
    FOREIGN KEY (transaksi_id) REFERENCES transaksi(transaksi_id),
    FOREIGN KEY (pengguna_id)  REFERENCES pengguna(pengguna_id)
);

INSERT INTO feedback (
    transaksi_id, pengguna_id, rating, komentar, kategori
)
VALUES
('TRX001',1,5,'Perjalanan sangat nyaman dan tepat waktu','Fasilitas Busway'),
('TRX002',2,4,'Cukup baik, hanya sedikit padat','Umum'),
('TRX003',3,5,'Bus bersih dan AC dingin','Fasilitas Busway'),
('TRX004',4,3,'Agak lama di halte','Umum'),
('TRX005',5,2,'Tidak sempat tap out, membingungkan','Aplikasi Jaklingko'),

('TRX006',6,5,'Sangat puas dengan pelayanan','Umum'),
('TRX007',7,4,'Lumayan nyaman','Fasilitas Busway'),
('TRX008',8,5,'Perjalanan lancar tanpa hambatan','Umum'),
('TRX009',9,3,'Cukup padat di jam sibuk','Umum'),
('TRX010',10,2,'Aplikasi kurang responsif','Aplikasi Jaklingko'),

('TRX011',11,5,'Halte bersih dan rapi','Fasilitas Busway'),
('TRX012',12,4,'Pelayanan cukup baik','Umum'),
('TRX013',13,5,'Sangat nyaman','Fasilitas Busway'),
('TRX014',14,3,'Waktu tunggu agak lama','Umum'),
('TRX015',15,2,'Tidak ada info jelas di aplikasi','Aplikasi Jaklingko'),

('TRX016',16,5,'Bus tepat waktu','Umum'),
('TRX017',17,4,'Cukup memuaskan','Fasilitas Busway'),
('TRX018',18,5,'AC dingin dan kursi nyaman','Fasilitas Busway'),
('TRX019',19,3,'Lumayan','Umum'),
('TRX020',20,2,'Kurang informasi rute','Aplikasi Jaklingko'),

('TRX021',21,5,'Sangat bagus','Umum'),
('TRX022',22,4,'Pelayanan baik','Fasilitas Busway'),
('TRX023',23,5,'Perjalanan lancar','Umum'),
('TRX024',24,3,'Biasa saja','Umum'),
('TRX025',25,2,'Aplikasi sering error','Aplikasi Jaklingko'),

('TRX026',26,5,'Nyaman sekali','Fasilitas Busway'),
('TRX027',27,4,'Lumayan bagus','Umum'),
('TRX028',28,5,'Sangat cepat','Umum'),
('TRX029',29,3,'Agak ramai','Umum'),
('TRX030',30,2,'Kurang jelas di aplikasi','Aplikasi Jaklingko'),

('TRX031',31,5,'Pelayanan prima','Fasilitas Busway'),
('TRX032',32,4,'Cukup nyaman','Umum'),
('TRX033',33,5,'Sangat memuaskan','Fasilitas Busway'),
('TRX034',34,3,'Sedikit delay','Umum'),
('TRX035',35,2,'Sulit cek saldo','Aplikasi Jaklingko'),

('TRX036',36,5,'Bus bersih','Fasilitas Busway'),
('TRX037',37,4,'Baik','Umum'),
('TRX038',38,5,'Perjalanan cepat','Umum'),
('TRX039',39,3,'Standar','Umum'),
('TRX040',40,2,'UI aplikasi membingungkan','Aplikasi Jaklingko'),

('TRX041',41,5,'Sangat nyaman','Fasilitas Busway'),
('TRX042',42,4,'Cukup baik','Umum'),
('TRX043',43,5,'Top pelayanan','Fasilitas Busway'),
('TRX044',44,3,'Kurang cepat','Umum'),
('TRX045',45,2,'Aplikasi lambat','Aplikasi Jaklingko'),

('TRX046',46,5,'Excellent','Fasilitas Busway'),
('TRX047',47,4,'Good','Umum'),
('TRX048',48,5,'Very smooth trip','Umum'),
('TRX049',49,3,'Okay','Umum'),
('TRX050',50,2,'App sering crash','Aplikasi Jaklingko');

INSERT INTO pengguna (pengguna_id, nama_lengkap, bank_kartu, gender, tahun_lahir)
VALUES
(1,'Andi Saputra','brizzi','M',1995),
(2,'Budi Santoso','emoney','M',1992),
(3,'Citra Lestari','flazz','F',1998),
(4,'Dewi Anggraini','dki','F',2000),
(5,'Eko Prasetyo','bni','M',1990),
(6,'Fajar Nugroho','brizzi','M',1997),
(7,'Gita Permata','flazz','F',1999),
(8,'Hadi Wijaya','emoney','M',1993),
(9,'Intan Sari','dki','F',2001),
(10,'Joko Susilo','bni','M',1989),

(11,'Kiki Amelia','brizzi','F',1996),
(12,'Lukman Hakim','emoney','M',1994),
(13,'Maya Putri','flazz','F',1997),
(14,'Nanda Saputra','dki','M',2002),
(15,'Oki Setiawan','bni','M',1991),
(16,'Putri Ayu','brizzi','F',1998),
(17,'Qori Rahma','flazz','F',2000),
(18,'Rizky Hidayat','emoney','M',1995),
(19,'Salsa Billa','dki','F',2001),
(20,'Tono Gunawan','bni','M',1988),
(21,'Umar Faruq','brizzi','M',1993),
(22,'Vina Lestari','flazz','F',1999),
(23,'Wahyu Kurniawan','emoney','M',1992),
(24,'Xena Putri','dki','F',2003),
(25,'Yudi Hartono','bni','M',1990),
(26,'Zahra Nabila','brizzi','F',2001),
(27,'Agus Salim','emoney','M',1987),
(28,'Bella Sari','flazz','F',1996),
(29,'Cahyo Nugroho','dki','M',1994),
(30,'Dinda Permata','bni','F',1998),
(31,'Erwin Saputra','brizzi','M',1995),
(32,'Fitri Handayani','flazz','F',1999),
(33,'Gilang Ramadhan','emoney','M',1993),
(34,'Hana Oktavia','dki','F',2002),
(35,'Iqbal Maulana','bni','M',1997),
(36,'Jihan Aulia','brizzi','F',2000),
(37,'Kevin Pratama','flazz','M',1996),
(38,'Laila Sari','emoney','F',1998),
(39,'Miko Saputra','dki','M',1991),
(40,'Nisa Rahma','bni','F',1999),
(41,'Omar Dani','brizzi','M',1992),
(42,'Putra Wijaya','flazz','M',1995),
(43,'Qiana Lestari','emoney','F',2001),
(44,'Rina Kartika','dki','F',1997),
(45,'Sandi Firmansyah','bni','M',1990),
(46,'Tari Kusuma','brizzi','F',2002),
(47,'Udin Setiawan','flazz','M',1989),
(48,'Vicky Prasetyo','emoney','M',1994),
(49,'Wulan Sari','dki','F',2000),
(50,'Yoga Saputra','bni','M',1996);

INSERT INTO transaksi (
    transaksi_id, pengguna_id, rute_id,
    halte_naik_id, halte_turun_id,
    arah_perjalanan, urutan_naik, urutan_turun,
    waktu_tap_in, waktu_tap_out, total_bayar
)
VALUES
('TRX001',1,'R01','H01','H05',0,1,5,'2024-05-01 07:00','2024-05-01 07:30',3500),
('TRX002',2,'R02','H03','H07',0,3,7,'2024-05-01 07:10','2024-05-01 07:45',3500),
('TRX003',3,'R01','H02','H06',1,2,6,'2024-05-01 08:00','2024-05-01 08:35',3500),
('TRX004',4,'R03','H01','H08',0,1,8,'2024-05-01 08:15','2024-05-01 09:00',20000),
('TRX005',5,'R02','H04','H09',1,4,9,'2024-05-01 09:00',NULL,0),

('TRX006',6,'R01','H01','H05',0,1,5,'2024-05-02 07:00','2024-05-02 07:25',3500),
('TRX007',7,'R02','H02','H06',1,2,6,'2024-05-02 07:20','2024-05-02 07:55',3500),
('TRX008',8,'R03','H03','H07',0,3,7,'2024-05-02 08:10','2024-05-02 08:50',20000),
('TRX009',9,'R01','H01','H04',0,1,4,'2024-05-02 08:30','2024-05-02 09:00',3500),
('TRX010',10,'R02','H05','H09',1,5,9,'2024-05-02 09:00',NULL,0),

('TRX011',11,'R01','H02','H06',0,2,6,'2024-05-03 07:00','2024-05-03 07:30',3500),
('TRX012',12,'R02','H03','H08',0,3,8,'2024-05-03 07:15','2024-05-03 07:50',3500),
('TRX013',13,'R03','H01','H07',1,1,7,'2024-05-03 08:00','2024-05-03 08:45',20000),
('TRX014',14,'R01','H04','H09',0,4,9,'2024-05-03 08:20','2024-05-03 09:10',3500),
('TRX015',15,'R02','H02','H05',1,2,5,'2024-05-03 09:00',NULL,0),

('TRX016',16,'R01','H01','H05',0,1,5,'2024-05-04 07:00','2024-05-04 07:20',3500),
('TRX017',17,'R02','H03','H07',1,3,7,'2024-05-04 07:25','2024-05-04 08:00',3500),
('TRX018',18,'R03','H02','H08',0,2,8,'2024-05-04 08:10','2024-05-04 08:55',20000),
('TRX019',19,'R01','H01','H04',0,1,4,'2024-05-04 08:40','2024-05-04 09:05',3500),
('TRX020',20,'R02','H05','H09',1,5,9,'2024-05-04 09:10',NULL,0),

('TRX021',21,'R01','H02','H06',0,2,6,'2024-05-05 07:05','2024-05-05 07:35',3500),
('TRX022',22,'R02','H03','H08',0,3,8,'2024-05-05 07:20','2024-05-05 07:55',3500),
('TRX023',23,'R03','H01','H07',1,1,7,'2024-05-05 08:05','2024-05-05 08:45',20000),
('TRX024',24,'R01','H04','H09',0,4,9,'2024-05-05 08:30','2024-05-05 09:10',3500),
('TRX025',25,'R02','H02','H05',1,2,5,'2024-05-05 09:00',NULL,0),

('TRX026',26,'R01','H01','H05',0,1,5,'2024-05-06 07:00','2024-05-06 07:30',3500),
('TRX027',27,'R02','H03','H07',1,3,7,'2024-05-06 07:15','2024-05-06 07:50',3500),
('TRX028',28,'R03','H02','H08',0,2,8,'2024-05-06 08:10','2024-05-06 08:55',20000),
('TRX029',29,'R01','H01','H04',0,1,4,'2024-05-06 08:30','2024-05-06 09:00',3500),
('TRX030',30,'R02','H05','H09',1,5,9,'2024-05-06 09:10',NULL,0),

('TRX031',31,'R01','H02','H06',0,2,6,'2024-05-07 07:05','2024-05-07 07:35',3500),
('TRX032',32,'R02','H03','H08',0,3,8,'2024-05-07 07:20','2024-05-07 07:55',3500),
('TRX033',33,'R03','H01','H07',1,1,7,'2024-05-07 08:05','2024-05-07 08:45',20000),
('TRX034',34,'R01','H04','H09',0,4,9,'2024-05-07 08:30','2024-05-07 09:10',3500),
('TRX035',35,'R02','H02','H05',1,2,5,'2024-05-07 09:00',NULL,0),

('TRX036',36,'R01','H01','H05',0,1,5,'2024-05-08 07:00','2024-05-08 07:25',3500),
('TRX037',37,'R02','H03','H07',1,3,7,'2024-05-08 07:15','2024-05-08 07:50',3500),
('TRX038',38,'R03','H02','H08',0,2,8,'2024-05-08 08:10','2024-05-08 08:55',20000),
('TRX039',39,'R01','H01','H04',0,1,4,'2024-05-08 08:30','2024-05-08 09:00',3500),
('TRX040',40,'R02','H05','H09',1,5,9,'2024-05-08 09:10',NULL,0),

('TRX041',41,'R01','H02','H06',0,2,6,'2024-05-09 07:05','2024-05-09 07:35',3500),
('TRX042',42,'R02','H03','H08',0,3,8,'2024-05-09 07:20','2024-05-09 07:55',3500),
('TRX043',43,'R03','H01','H07',1,1,7,'2024-05-09 08:05','2024-05-09 08:45',20000),
('TRX044',44,'R01','H04','H09',0,4,9,'2024-05-09 08:30','2024-05-09 09:10',3500),
('TRX045',45,'R02','H02','H05',1,2,5,'2024-05-09 09:00',NULL,0),

('TRX046',46,'R01','H01','H05',0,1,5,'2024-05-10 07:00','2024-05-10 07:25',3500),
('TRX047',47,'R02','H03','H07',1,3,7,'2024-05-10 07:15','2024-05-10 07:50',3500),
('TRX048',48,'R03','H02','H08',0,2,8,'2024-05-10 08:10','2024-05-10 08:55',20000),
('TRX049',49,'R01','H01','H04',0,1,4,'2024-05-10 08:30','2024-05-10 09:00',3500),
('TRX050',50,'R02','H05','H09',1,5,9,'2024-05-10 09:10',NULL,0);
