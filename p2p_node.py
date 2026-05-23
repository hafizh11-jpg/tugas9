"""
=============================================================
  Distributed File Indexing - P2P Node
  Tugas #9 - Socket Programming & Threading
=============================================================
  Cara menjalankan:
    python p2p_node.py <IP_ANDA> <PORT>
  Contoh:
    python p2p_node.py 192.168.1.10 5001   <- Node A
    python p2p_node.py 192.168.1.11 5002   <- Node B
    python p2p_node.py 192.168.1.12 5003   <- Node C
=============================================================
"""

import socket
import threading
import json
import sys
import os
import time
from datetime import datetime

# ─────────────────────────────────────────────
#  KONFIGURASI AWAL
# ─────────────────────────────────────────────
# Edit daftar IP:Port peer di sini sesuai kondisi jaringan Anda
# atau gunakan file config.json (otomatis dibaca jika ada)
DEFAULT_KNOWN_PEERS = [
    # ("192.168.1.10", 5001),   # Node A
    # ("192.168.1.11", 5002),   # Node B
    # ("192.168.1.12", 5003),   # Node C
]

BUFFER_SIZE  = 4096
TIMEOUT_SECS = 2        # Percobaan 2: timeout agar tidak hang saat peer mati
LOG_FILE     = "node_activity.log"

# ─────────────────────────────────────────────
#  WARNA TERMINAL (ANSI)
# ─────────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    RED    = "\033[91m"
    CYAN   = "\033[96m"
    BLUE   = "\033[94m"
    PURPLE = "\033[95m"


# ─────────────────────────────────────────────
#  UTILITAS
# ─────────────────────────────────────────────
def ts():
    """Timestamp singkat untuk log."""
    return datetime.now().strftime("%H:%M:%S")


def log(level: str, msg: str, color: str = C.RESET):
    line = f"[{ts()}] [{level}] {msg}"
    print(color + line + C.RESET)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ─────────────────────────────────────────────
#  CLASS UTAMA: P2PNode
# ─────────────────────────────────────────────
class P2PNode:
    def __init__(self, host: str, port: int):
        self.host        = host
        self.port        = port
        self.node_id     = f"{host}:{port}"
        self.shared_files: list[str] = []   # daftar file yang di-register
        self.known_peers: list[tuple] = []   # daftar (ip, port) peer
        self.search_counter = 0              # Percobaan 3: hitung pesan SEARCH masuk
        self.lock = threading.Lock()         # Sinkronisasi akses shared_files

        self._load_config()
        self._banner()

    # ── Muat konfigurasi dari config.json (jika ada) ──────────────────────
    def _load_config(self):
        if os.path.exists("config.json"):
            with open("config.json") as f:
                cfg = json.load(f)
            self.known_peers = [(p["ip"], p["port"]) for p in cfg.get("peers", [])]
            # Tambahkan DEFAULT_KNOWN_PEERS yang belum ada
            for peer in DEFAULT_KNOWN_PEERS:
                if peer not in self.known_peers:
                    self.known_peers.append(peer)
            log("CONFIG", f"Memuat {len(self.known_peers)} peer dari config.json", C.CYAN)
        else:
            self.known_peers = list(DEFAULT_KNOWN_PEERS)
            log("CONFIG", "config.json tidak ditemukan, menggunakan DEFAULT_KNOWN_PEERS", C.YELLOW)

        # Hapus diri sendiri dari daftar peer
        self.known_peers = [
            (ip, p) for ip, p in self.known_peers
            if not (ip == self.host and p == self.port)
        ]

    def _banner(self):
        print(C.CYAN + C.BOLD)
        print("╔══════════════════════════════════════════╗")
        print(f"║   P2P Node  ►  {self.node_id:<26} ║")
        print(f"║   Peers terdaftar: {len(self.known_peers):<23} ║")
        print("╚══════════════════════════════════════════╝")
        print(C.RESET)

    # ══════════════════════════════════════════
    #  FITUR 1 – register_file
    # ══════════════════════════════════════════
    def register_file(self, filename: str):
        """Tambahkan file ke daftar sharing lokal."""
        with self.lock:
            if filename not in self.shared_files:
                self.shared_files.append(filename)
                log("REGISTER", f"File '{filename}' berhasil didaftarkan.", C.GREEN)
            else:
                log("REGISTER", f"File '{filename}' sudah ada dalam daftar.", C.YELLOW)

    # ══════════════════════════════════════════
    #  FITUR 2 – search_file
    # ══════════════════════════════════════════
    def search_file(self, filename: str):
        """Kirim query SEARCH ke semua peer yang terdaftar (broadcast)."""
        if not self.known_peers:
            log("SEARCH", "Tidak ada peer terdaftar!", C.RED)
            return

        log("SEARCH", f"Mencari '{filename}' ke {len(self.known_peers)} peer...", C.BLUE)
        threads = []
        for (peer_ip, peer_port) in self.known_peers:
            t = threading.Thread(
                target=self._send_search,
                args=(peer_ip, peer_port, filename),
                daemon=True
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

    def _send_search(self, peer_ip: str, peer_port: int, filename: str):
        """(Thread) Kirim satu pesan SEARCH ke peer tertentu."""
        msg = json.dumps({
            "type"    : "SEARCH",
            "filename": filename,
            "from_ip" : self.host,
            "from_port": self.port
        })
        try:
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.settimeout(TIMEOUT_SECS)   # <── kunci Percobaan 2
            c.connect((peer_ip, peer_port))
            c.sendall(msg.encode())
            raw = c.recv(BUFFER_SIZE)
            c.close()
            if raw:
                self.response_handler(json.loads(raw.decode()), peer_ip, peer_port)
        except socket.timeout:
            log("ERROR", f"Timeout menghubungi {peer_ip}:{peer_port} (node mungkin mati)", C.RED)
        except ConnectionRefusedError:
            log("ERROR", f"Koneksi ditolak oleh {peer_ip}:{peer_port}", C.RED)
        except Exception as e:
            log("ERROR", f"Gagal menghubungi {peer_ip}:{peer_port} – {e}", C.RED)

    # ══════════════════════════════════════════
    #  FITUR 3 – response_handler
    # ══════════════════════════════════════════
    def response_handler(self, data: dict, peer_ip: str, peer_port: int):
        """Proses balasan RESPONSE dari peer."""
        if data.get("found"):
            log(
                "RESPONSE",
                f"✔ File '{data['filename']}' ditemukan di {data['owner_ip']}:{data['owner_port']}",
                C.GREEN
            )
        else:
            log(
                "RESPONSE",
                f"✘ File '{data['filename']}' tidak ada di {peer_ip}:{peer_port}",
                C.YELLOW
            )

    # ══════════════════════════════════════════
    #  SERVER – menerima koneksi masuk
    # ══════════════════════════════════════════
    def start_server(self):
        """Jalankan server socket di background thread."""
        t = threading.Thread(target=self._listen, daemon=True)
        t.start()
        log("SERVER", f"Mendengarkan di {self.host}:{self.port}", C.PURPLE)

    def _listen(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(10)
        while True:
            conn, addr = srv.accept()
            threading.Thread(
                target=self._handle_connection,
                args=(conn, addr),
                daemon=True
            ).start()

    def _handle_connection(self, conn: socket.socket, addr):
        """(Thread) Proses satu koneksi masuk."""
        try:
            raw  = conn.recv(BUFFER_SIZE)
            data = json.loads(raw.decode())

            if data["type"] == "SEARCH":
                with self.lock:
                    self.search_counter += 1   # Percobaan 3: tambah counter
                filename = data["filename"]
                log("REQUEST", f"Query '{filename}' dari {addr[0]}:{addr[1]}", C.CYAN)

                found = filename in self.shared_files
                reply = json.dumps({
                    "found"     : found,
                    "filename"  : filename,
                    "owner_ip"  : self.host,
                    "owner_port": self.port
                })
                conn.sendall(reply.encode())
                log(
                    "REPLY",
                    f"{'✔ FOUND' if found else '✘ NOT FOUND'} → '{filename}'",
                    C.GREEN if found else C.YELLOW
                )
        except Exception as e:
            log("ERROR", f"Kesalahan handle koneksi: {e}", C.RED)
        finally:
            conn.close()

    # ══════════════════════════════════════════
    #  CLI INTERAKTIF
    # ══════════════════════════════════════════
    def show_status(self):
        print(C.BOLD + "\n── STATUS NODE ─────────────────────────────" + C.RESET)
        print(f"  Node ID     : {self.node_id}")
        print(f"  File lokal  : {self.shared_files or '(kosong)'}")
        print(f"  Peer aktif  : {self.known_peers or '(kosong)'}")
        print(f"  Search masuk: {self.search_counter} pesan")
        print()

    def run(self):
        self.start_server()
        print(C.BOLD + "\nMenu:" + C.RESET)
        print("  1. Search file di jaringan")
        print("  2. Register file lokal")
        print("  3. Tampilkan status")
        print("  4. Keluar\n")

        while True:
            try:
                choice = input(C.BOLD + "Pilih [1-4]: " + C.RESET).strip()
            except (KeyboardInterrupt, EOFError):
                print("\nNode dimatikan.")
                break

            if choice == "1":
                fname = input("  Nama file yang dicari: ").strip()
                if fname:
                    self.search_file(fname)

            elif choice == "2":
                fname = input("  Nama file yang ingin didaftarkan: ").strip()
                if fname:
                    self.register_file(fname)

            elif choice == "3":
                self.show_status()

            elif choice == "4":
                print("Node dimatikan.")
                break

            else:
                print(C.YELLOW + "Pilihan tidak valid." + C.RESET)


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Penggunaan: python p2p_node.py <IP> <PORT>")
        print("Contoh   : python p2p_node.py 192.168.1.10 5001")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    node = P2PNode(host, port)
    node.run()
