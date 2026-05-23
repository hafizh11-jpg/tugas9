# Distributed File Indexing – P2P Node
**Tugas #9 | Socket Programming & Threading**

---

## Cara Cepat Menjalankan (3 Terminal / 3 Notebook)

### 1. Edit IP di `config.json`
Ganti IP sesuai IP lokal masing-masing notebook:
```json
{
  "peers": [
    { "ip": "192.168.1.10", "port": 5001 },
    { "ip": "192.168.1.11", "port": 5002 },
    { "ip": "192.168.1.12", "port": 5003 }
  ]
}
```

### 2. Jalankan tiap node
```bash
# Notebook Node A
python p2p_node.py 192.168.1.10 5001

# Notebook Node B
python p2p_node.py 192.168.1.11 5002

# Notebook Node C
python p2p_node.py 192.168.1.12 5003
```

---

## Menu Interaktif
| Pilihan | Aksi |
|---------|------|
| 1 | Search file ke semua peer (broadcast) |
| 2 | Register file lokal |
| 3 | Tampilkan status node |
| 4 | Keluar |

---

## Skenario Uji Coba

### Percobaan 1 – Baseline
1. Jalankan Node A, B, C.
2. Di Node B dan C, pilih **2** → register `DataRiset.pdf`.
3. Di Node A, pilih **1** → cari `DataRiset.pdf`.
4. Amati log `[REQUEST]` di terminal B & C dan log `[RESPONSE]` di terminal A.

### Percobaan 2 – Fault Tolerance
1. Jalankan semua node, lakukan search untuk verifikasi.
2. Matikan Node C dengan `CTRL+C`.
3. Dari Node A, lakukan search lagi.
4. Amati log `[ERROR] Timeout` – node A **tidak crash** berkat `c.settimeout(2)`.

### Percobaan 3 – Traffic Analysis
1. Jalankan semua node.
2. Dari Node A, lakukan **5× search** berturut-turut.
3. Di Node B atau C, pilih **3** → lihat `Search masuk: X pesan`.

---

## Struktur File
```
p2p_project/
├── p2p_node.py        ← kode utama (jalankan ini)
├── config.json        ← daftar IP peer (edit sesuai jaringan)
├── node_activity.log  ← log aktivitas (dibuat otomatis)
└── README.md
```
