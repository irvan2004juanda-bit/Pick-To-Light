# Database Schema — Pick-to-Light System V2

Database MySQL untuk sistem PTL V2, berisi 6 tabel yang menyimpan 
data master barang dan data mentah pengujian sistem.

## Struktur Tabel

| Tabel | Deskripsi | Digunakan di Skripsi |
|---|---|---|
| item_data | Master 16 barang | Batasan Masalah poin 9 |
| orders | Log semua order | Bab IV 4.2.1 (Blackbox) |
| delay_test | Data delay komunikasi (sesi 3 Juli 2026) | Tabel 4.3 |
| payload_log | Data ukuran payload MQTT | Sub-bab 4.2.4 |
| performance_test | Data throughput MQTT (sesi 15 Juli 2026) | Tabel 4.4 |
| picking_log | Log picking sukses/gagal | Sub-bab 4.2.2 |

## Cara Import

### Via phpMyAdmin
1. Buka http://localhost/phpmyadmin
2. Buat database baru: ptl_db
3. Pilih database ptl_db, klik tab Import
4. Upload file ptl_db.sql, klik Go

### Via Command Line
mysql -u root -p ptl_db < ptl_db.sql

## Statistik Data

- Total rows: 1.511
- Total size: ~320 KB
- Character set: UTF-8 (utf8mb4)
- Engine: InnoDB
