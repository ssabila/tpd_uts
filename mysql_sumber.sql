-- ============================================================
--  JAKLINGKO DATA PIPELINE – UTS
--  FILE    : mysql_sumber.sql
--  DATABASE: jaklingko_mysql  (Sumber 1 – MySQL Workbench)
--  DATASET : dfTransjakarta.csv (Kaggle, April 2023)
--
--  CATATAN ARMADA:
--    Dataset Kaggle tidak memiliki kolom armada/kendaraan.
--    armada_id dibuat SINTETIS dengan format:
--      ARM-{corridorID}-{direction}-{YYYYMMDD}
--    Contoh: ARM-5-1-20230403, ARM-R1A-0-20230403
--
--    Logika: 1 bus melayani 1 koridor, 1 arah, per hari
--    rata-rata 3.5 penumpang per armada di data Kaggle
-- ============================================================

CREATE DATABASE IF NOT EXISTS jaklingko_mysql
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE jaklingko_mysql;

CREATE TABLE IF NOT EXISTS rute (
    rute_id       VARCHAR(20)   NOT NULL,
    nama_rute     VARCHAR(200)  NOT NULL,
    asal          VARCHAR(100),
    tujuan        VARCHAR(150),
    jenis_koridor ENUM('BRT Utama','BRT Cabang','Royaltrans','Mikrotrans',
                       'Jak Lingko','Transjabodetabek','Lainnya')
                               NOT NULL DEFAULT 'BRT Utama',
    status        ENUM('Aktif','Nonaktif') NOT NULL DEFAULT 'Aktif',
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                           ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (rute_id),
    INDEX idx_jenis_koridor (jenis_koridor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS halte (
    halte_id    VARCHAR(20)   NOT NULL,
    nama_halte  VARCHAR(150)  NOT NULL,
    latitude    DECIMAL(10,7) NOT NULL,
    longitude   DECIMAL(10,7) NOT NULL,
    status      ENUM('Aktif','Nonaktif') NOT NULL DEFAULT 'Aktif',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (halte_id),
    INDEX idx_koordinat (latitude, longitude)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS armada (
    armada_id        VARCHAR(40)  NOT NULL
                     COMMENT 'Sintetis: ARM-{corridorID}-{direction}-{YYYYMMDD}',
    rute_id          VARCHAR(20)  NOT NULL,
    arah_perjalanan  TINYINT      NOT NULL COMMENT '0=pergi, 1=pulang',
    tanggal_operasi  DATE         NOT NULL,
    jenis_armada     VARCHAR(60)  NOT NULL COMMENT 'Estimasi dari jenis koridor',
    kapasitas        SMALLINT     NOT NULL COMMENT 'Estimasi kapasitas penumpang',
    catatan          VARCHAR(200) COMMENT 'armada sintetis dari data Kaggle',
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (armada_id),
    FOREIGN KEY (rute_id) REFERENCES rute(rute_id)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    INDEX idx_rute_tanggal (rute_id, tanggal_operasi)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Armada sintetis: 1 baris = 1 trip bus per koridor per arah per hari';


use jaklingko_mysql;

INSERT INTO rute (rute_id, nama_rute, asal, tujuan, jenis_koridor)
VALUES
('5','Kampung Melayu - Ancol','Kp Melayu','Ancol','BRT Utama'),
('6C','Stasiun Tebet - Karet','Tebet','Karet','BRT Cabang'),
('R1A','Bogor - Blok M','Bogor','Blok M','Transjabodetabek'),
('11D','Pulo Gebang - Pulogadung','Pulo Gebang','Pulogadung','BRT Cabang'),
('12','Pluit - Tanjung Priok','Pluit','Tj Priok','BRT Utama');

INSERT INTO halte (halte_id, nama_halte, latitude, longitude)
VALUES
('P00142','Halte 142',-6.200100,106.816000),
('P00253','Halte 253',-6.210200,106.820000),
('B01963P','Halte 1963',-6.215000,106.830000),
('B03307P','Halte 3307',-6.220000,106.835000),
('B00499P','Halte 499',-6.225000,106.840000),
('B04962P','Halte 4962',-6.230000,106.845000),
('B05587P','Halte 5587',-6.235000,106.850000),
('B03090P','Halte 3090',-6.240000,106.855000),
('P00239','Halte 239',-6.245000,106.860000),
('P00098','Halte 098',-6.250000,106.865000);

INSERT INTO armada (
    armada_id, rute_id, arah_perjalanan,
    tanggal_operasi, jenis_armada, kapasitas, catatan
)
VALUES
('ARM-5-1-20230403','5',1,'2023-04-03','Bus Besar',80,'synthetic dari transaksi'),
('ARM-6C-0-20230403','6C',0,'2023-04-03','Bus Sedang',60,'synthetic dari transaksi'),
('ARM-R1A-0-20230403','R1A',0,'2023-04-03','Bus Besar',80,'synthetic dari transaksi'),
('ARM-11D-0-20230403','11D',0,'2023-04-03','Bus Sedang',60,'synthetic dari transaksi'),
('ARM-12-0-20230403','12',0,'2023-04-03','Bus Besar',80,'synthetic dari transaksi'),

('ARM-5-1-20230404','5',1,'2023-04-04','Bus Besar',80,'synthetic'),
('ARM-6C-0-20230404','6C',0,'2023-04-04','Bus Sedang',60,'synthetic'),
('ARM-R1A-0-20230404','R1A',0,'2023-04-04','Bus Besar',80,'synthetic'),
('ARM-11D-1-20230404','11D',1,'2023-04-04','Bus Sedang',60,'synthetic'),
('ARM-12-0-20230404','12',0,'2023-04-04','Bus Besar',80,'synthetic'),

('ARM-5-1-20230405','5',1,'2023-04-05','Bus Besar',80,'synthetic'),
('ARM-6C-0-20230405','6C',0,'2023-04-05','Bus Sedang',60,'synthetic'),
('ARM-R1A-0-20230405','R1A',0,'2023-04-05','Bus Besar',80,'synthetic'),
('ARM-11D-1-20230405','11D',1,'2023-04-05','Bus Sedang',60,'synthetic'),
('ARM-12-0-20230405','12',0,'2023-04-05','Bus Besar',80,'synthetic'),

('ARM-5-1-20230406','5',1,'2023-04-06','Bus Besar',80,'synthetic'),
('ARM-6C-0-20230406','6C',0,'2023-04-06','Bus Sedang',60,'synthetic'),
('ARM-R1A-0-20230406','R1A',0,'2023-04-06','Bus Besar',80,'synthetic'),
('ARM-11D-1-20230406','11D',1,'2023-04-06','Bus Sedang',60,'synthetic'),
('ARM-12-0-20230406','12',0,'2023-04-06','Bus Besar',80,'synthetic');