import os
import re
import json
import threading
import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# ─── 1. LOAD CONFIGURATION ────────────────────────────────────────────────────
load_dotenv()
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql://user:password@host:5432/dbname")
DINOIKI_API_KEY = os.getenv("DINOIKI_API_KEY", "")
FONNTE_TOKEN    = os.getenv("FONNTE_TOKEN", "")
DINOIKI_URL     = "https://ai.dinoiki.com/v1/chat/completions"
AI_MODEL        = "gpt-4o"

# ─── 2. STOPWORDS ─────────────────────────────────────────────────────────────
STOPWORDS = {
    'apa', 'siapa', 'berapa', 'yang', 'dan', 'atau', 'dari', 'untuk',
    'dengan', 'adalah', 'ini', 'itu', 'ada', 'tidak', 'bisa', 'mau',
    'saya', 'kamu', 'dia', 'kami', 'kita', 'mereka', 'semua', 'sudah',
    'belum', 'sedang', 'akan', 'telah', 'pada', 'di', 'ke', 'oleh',
    'juga', 'hanya', 'lebih', 'paling', 'sangat', 'banyak', 'sedikit',
    'tampilkan', 'tunjukkan', 'cari', 'lihat', 'data', 'info', 'informasi',
    'list', 'daftar', 'total', 'jumlah', 'nilai', 'status', 'semua',
    'kontrak', 'vendor', 'tagihan', 'dokumen', 'progress', 'bulan', 'tahun'
}

# ─── 3. DATABASE SCHEMA CONTEXT ───────────────────────────────────────────────
SCHEMA_CONTEXT = """
Database PostgreSQL untuk sistem manajemen kontrak kilang minyak. Berikut skema tabel:

TABEL: profiles
Kolom: id, email, full_name, role (admin/pic/user/vendor/external), password_hash, created_at, updated_at, is_active, id_vendor

TABEL: vendor
Kolom: id_vendor, nama_vendor, npwp, alamat, pic_nama, pic_kontak, status_vendor (Active/Inactive/Blacklist), score, created_at, updated_at

TABEL: direksi_pekerjaan
Kolom: id_direksi_pekerjaan, nama, jabatan (Manager Maintenance Execution I/Manager Maintenance Execution II/Pengawas Pekerjaan), sub_area (SH Maintenance Area 5/SH Maintenance Area 6/SH Maintenance Area 7/Workshop), created_at, updated_at
Catatan: Tabel master nama & jabatan direksi/pengawas. Kolom direksi_pekerjaan di tabel kontrak berisi NAMA ORANG (string bebas), bukan kode area seperti MA5/MA6.

TABEL: program_kerja
Kolom: id_program_kerja, nama (Rutin/Non Rutin/TA/OH), created_at, updated_at

TABEL: planner
Kolom: id_planner, nama (P&S/OH/TA), created_at, updated_at

TABEL: kontrak
Kolom: id_kontrak, id_vendor, judul_kontrak, no_dokumen_kontrak, no_po_pr, no_irkap, direksi_pekerjaan,
  program_kerja, planner, kbo_bagian,
  tipe_kontrak (Lumpsum/Unit Price/TSA/LTSA/TSA/LTSA), status_kontrak (Pre-KOM/Aktif/Active/Selesai/Completed/Terminated),
  tanggal_spb_diterima, tanggal_terima_dokumen, tanggal_maksimal_kom, tanggal_mulai, tanggal_selesai,
  sla_kom_hari, estimasi_tanggal_kom, tanggal_kom, kom_terlambat, nilai_awal, durasi_kontrak_hari,
  progress_plan, progress_actual, aktivitas_saat_ini, kendala, disiplin, tkdn_percentage, tanggal_lkp,
  tanggal_mpl, tanggal_mpa, masa_pemeliharaan_hari,
  has_amendment, no_amandemen, tanggal_amandemen, jenis_amandemen, nilai_kontrak_baru, durasi_amandemen,
  tanggal_mulai_baru, tanggal_selesai_baru, alasan_perubahan,
  contract_documents (JSON), amendment_documents (JSON), s_curve_data (JSON),
  created_at, updated_at

TABEL: amandemen_kontrak
Kolom: id_amandemen, id_kontrak, nomor_urut, no_amandemen, tanggal_amandemen, jenis_amandemen,
  nilai_kontrak_baru, durasi_amandemen, tanggal_mulai_baru, tanggal_selesai_baru, alasan_perubahan,
  amendment_documents (JSON), created_at, updated_at

TABEL: tagihan
Kolom: id_tagihan, id_kontrak, nomor_tagihan, tanggal_tagihan, tipe_kontrak, termin, nilai_tagihan,
  status_tagihan, memo_required, tanggal_pengiriman_memo, dokumen_memo, dokumen_tagihan, catatan, created_at, updated_at

TABEL: sla_tagihan
Kolom: id, id_kontrak, id_tagihan,
  tgl_masuk_ba_joint_inspection, tgl_selesai_ba_joint_inspection,
  tgl_masuk_ba_commissioning, tgl_selesai_ba_commissioning,
  tgl_masuk_ba_penerimaan_material, tgl_selesai_ba_penerimaan_material,
  tgl_masuk_lkp, tgl_selesai_lkp,
  tgl_masuk_bast, tgl_selesai_bast,
  tgl_masuk_bakp, tgl_selesai_bakp,
  tgl_masuk_ivendor, tgl_selesai_ivendor,
  tgl_masuk_sa, tgl_selesai_sa,
  tgl_masuk_pa, tgl_selesai_pa,
  tgl_masuk_verifikasi, tgl_selesai_verifikasi,
  tgl_masuk_payment, tgl_selesai_payment,
  created_at, updated_at
Catatan: Tracking tanggal masuk & selesai per tahap untuk satu tagihan.

TABEL: sla_setting
Kolom: kode_tahap, batas_hari, warning_persen
Catatan: Konfigurasi batas hari dan ambang peringatan (%) per tahap SLA tagihan.

TABEL: progress_lumpsum
Kolom: id_progress, id_kontrak, milestone, persen, tanggal_update, evidence, created_at

TABEL: progress_unit_price
Kolom: id_progress, id_kontrak, nama_item, satuan, qty_rencana, qty_aktual, harga_satuan, tanggal_update, created_at

TABEL: monitoring_ltsa
Kolom: id_log, id_kontrak, tanggal_kunjungan, jenis_layanan (Preventive/Corrective/Standby),
  durasi_jam, sla_terpenuhi (Yes/No), keterangan, created_at

TABEL: padi
Kolom: id_padi, no_pembelian, tanggal, judul_pembelian, no_po_pr, nilai, id_vendor, link_pembelian,
  bagian, dokumen_pendukung, status_purchase (BAST), tanggal_bast, tanggal_sa_gr, tanggal_invoice,
  tanggal_payment_approval, tanggal_paid, catatan_status, created_at, updated_at

TABEL: dokumen_approval
Kolom: id_dokumen, id_kontrak, tipe_dokumen (Evident/Report/Persetujuan), nama_dokumen,
  deskripsi_dokumen, file_path, file_url, nama_file, tipe_file, ukuran_file,
  status_approval (Pending/Approved/Rejected), catatan_reviewer,
  uploaded_by, reviewed_by, reviewed_at, created_at, updated_at

TABEL: daily_report
Kolom: id_report, tanggal_laporan, disiplin, kategori, deskripsi, direksi, tag_number,
  status_pekerjaan, catatan, pengirim_wa, raw_text, created_at
Catatan: Laporan harian kegiatan maintenance. Diisi via fitur #laporan di WhatsApp.

TABEL: konfigurasi_sistem
Kolom: id_setting, nama_setting, nilai_setting, deskripsi, updated_at

Relasi penting:
- vendor.id_vendor -> kontrak.id_vendor (1 vendor banyak kontrak)
- kontrak.id_kontrak -> tagihan.id_kontrak
- kontrak.id_kontrak -> sla_tagihan.id_kontrak
- tagihan.id_tagihan -> sla_tagihan.id_tagihan
- kontrak.id_kontrak -> amandemen_kontrak.id_kontrak
- kontrak.id_kontrak -> progress_lumpsum.id_kontrak
- kontrak.id_kontrak -> progress_unit_price.id_kontrak
- kontrak.id_kontrak -> monitoring_ltsa.id_kontrak
- kontrak.id_kontrak -> dokumen_approval.id_kontrak
- vendor.id_vendor -> padi.id_vendor

NILAI ENUM & PILIHAN YANG VALID:

1. TIPE KONTRAK: 'Lumpsum', 'Unit Price', 'TSA', 'LTSA', 'TSA/LTSA'

2. STATUS KONTRAK: 'Pre-KOM', 'Aktif', 'Active', 'Selesai', 'Completed', 'Terminated'
   PENTING: 'Aktif' dan 'Active' adalah sinonim — gunakan OR saat filter status aktif.
   Begitu pula 'Selesai' dan 'Completed'. Contoh: WHERE status_kontrak IN ('Aktif', 'Active')

3. DISIPLIN: 'Instrument', 'Instrumentasi', 'Stationary', 'Electrical', 'Rotating', 'Alat Berat'
   PENTING: 'Instrument' dan 'Instrumentasi' adalah nilai yang sama — gunakan ILIKE '%instru%' atau OR.

4. DIREKSI PEKERJAAN: Field kontrak.direksi_pekerjaan berisi NAMA ORANG/JABATAN (bukan kode MA5/MA6).
   Untuk filter berdasarkan area, JOIN ke tabel direksi_pekerjaan dan filter kolom sub_area.
   Sub_area valid: 'SH Maintenance Area 5', 'SH Maintenance Area 6', 'SH Maintenance Area 7', 'Workshop'

5. JENIS AMANDEMEN: 'Nilai', 'Waktu', 'Nilai dan Waktu'

6. STATUS APPROVAL: 'Pending', 'Approved', 'Rejected'

7. STATUS VENDOR: 'Active', 'Inactive', 'Blacklist'

8. JENIS LAYANAN LTSA: 'Preventive', 'Corrective', 'Standby'

9. TAHAPAN SLA TAGIHAN — 11 tahap (kolom di tabel sla_tagihan, format: tgl_masuk_X / tgl_selesai_X):
   1. ba_joint_inspection     2. ba_commissioning        3. ba_penerimaan_material
   4. lkp                     5. bast                    6. bakp
   7. ivendor                 8. sa                      9. pa
   10. verifikasi             11. payment
   Tahap dianggap selesai jika tgl_selesai_X tidak NULL. Durasi = tgl_selesai - tgl_masuk.
   Bandingkan dengan batas_hari di tabel sla_setting (kode_tahap sesuai nama tahap).

10. KATEGORI DAILY REPORT: 'Corrective Maintenance', 'Preventive Maintenance', 'Plant Patrol',
    'Progress', 'Challenge Session', 'Support'

11. STATUS PEKERJAAN (daily_report): 'Done', 'In Progress', 'Waiting Material', 'Pending', '-'

12. PROGRAM KERJA: 'Rutin', 'Non Rutin', 'TA', 'OH'

13. PLANNER: 'P&S', 'OH', 'TA'
"""

# ─── 4. SYSTEM PROMPT ─────────────────────────────────────────────────────────
BASE_SYSTEM_PROMPT = (
    "Kamu adalah asisten cerdas untuk sistem manajemen kontrak kilang minyak, "
    "yang menjawab pertanyaan via WhatsApp.\n"
    "Kamu dapat menjawab pertanyaan bisnis dalam Bahasa Indonesia secara natural "
    "dan mengkonversinya ke query SQL PostgreSQL.\n\n"
    + SCHEMA_CONTEXT +
    "\nATURAN QUERY SQL:\n"
    "1. HANYA boleh generate query SELECT — TIDAK boleh UPDATE, DELETE, INSERT, DROP, ALTER, TRUNCATE\n"
    "2. TIDAK boleh SELECT * — selalu tentukan kolom yang relevan\n"
    "3. Selalu gunakan LIMIT maksimal 50 baris\n"
    "4. Gunakan JOIN yang tepat antar tabel\n"
    "5. Format angka nilai kontrak dalam format Indonesia (Rp)\n"
    "\nATURAN INTERPRETASI ENTITAS:\n"
    "- Jika ada blok 'KONTEKS ENTITAS YANG DITEMUKAN DI DATABASE' -> gunakan langsung, JANGAN minta klarifikasi\n"
    "- Jika user menyebut nama yang diawali PT/CV/UD -> cari di vendor.nama_vendor\n"
    "- Jika user menyebut kode seperti MA5, KOM-001 -> cari di direksi_pekerjaan atau no_dokumen_kontrak\n"
    "- Jika entitas tidak ditemukan di konteks -> baru boleh minta klarifikasi\n"
    "\nATURAN FORMAT JAWABAN (KHUSUS WHATSAPP — NARASI SAJA):\n"
    "1. JAWABAN FULL NARASI — JANGAN gunakan tabel HTML, JANGAN format markdown [CHART]\n"
    "2. Jika hasil lebih dari 10 item, tampilkan ringkasan/highlight saja, maksimal 5-7 poin\n"
    "3. Gunakan poin-poin dengan tanda • jika data lebih dari satu\n"
    "4. Tebalkan poin penting dengan *teks* (bold WhatsApp)\n"
    "5. Tambahkan emoticon relevan (📋, 💰, 📊, ✅, ⚠️, 🔧, 🏭, 🚨, 🔴, 🟢)\n"
    "6. Gunakan angka dengan format mudah dibaca (contoh: Rp 1.250.000.000 atau Rp 1,25 M)\n"
    "7. Jika data panjang, akhiri dengan: _(Menampilkan highlight, tanya lebih spesifik untuk detail)_\n"
    "8. DETEKSI PERTANYAAN TIDAK PRODUKTIF:\n"
    "   - 'tampilkan semua', 'list semua', 'dump data' -> tolak sopan, minta pertanyaan lebih spesifik\n"
    "   - Pertanyaan di luar konteks monitoring kontrak -> jawab: 'Maaf, saya hanya membantu analisis data kontrak kilang.'\n"
    "   PENGECUALIAN — tetap jawab ramah untuk:\n"
    "   * Sapaan (halo, selamat pagi, dsb) -> balas ramah\n"
    "   * Tanya kemampuan AI -> jelaskan apa saja yang bisa dibantu\n"
    "   * Ucapan terima kasih -> balas sopan\n"
    "\nFORMAT RESPONS JSON:\n"
    "Kamu HARUS selalu merespons dalam format JSON seperti ini:\n"
    "{\n"
    '  "type": "query" | "clarification" | "narrative" | "error",\n'
    '  "sql": "query SQL jika type=query, null jika tidak",\n'
    '  "explanation": "penjelasan singkat apa yang akan dilakukan",\n'
    '  "narrative_hint": "bagaimana cara menarasikan hasilnya",\n'
    '  "clarification_question": "pertanyaan klarifikasi jika type=clarification",\n'
    '  "message": "pesan untuk user dalam format WhatsApp"\n'
    "}\n"
)

# ─── 5. HELPER: CALL AI ────────────────────────────────────────────────────────
def call_ai(messages: list, max_tokens: int = 1500) -> str:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DINOIKI_API_KEY}"
    }
    payload = {
        "model": AI_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }
    resp = requests.post(DINOIKI_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()

# ─── 6. SMART ENTITY SEARCH ───────────────────────────────────────────────────
def smart_entity_search(user_message: str) -> str:
    """
    Cari entitas yang disebut user secara dinamis di database.
    Hasilnya diinjeksikan ke system prompt agar AI tahu konteksnya
    tanpa perlu tanya balik ke user.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        context   = []
        found_ids = set()

        # Ekstrak kata-kata penting (bukan stopword, minimal 3 huruf)
        words = [w for w in re.findall(r'\b\w{3,}\b', user_message)
                 if w.lower() not in STOPWORDS]

        # Tangkap pola PT/CV/UD/TB/PD secara khusus
        company_patterns = re.findall(
            r'\b(?:PT|CV|UD|TB|PD)\s+[\w\s]+', user_message, re.IGNORECASE
        )
        search_terms = list(set(words + company_patterns))

        for term in search_terms:
            term = term.strip()
            if len(term) < 3:
                continue

            # ── Cari di tabel vendor ──────────────────────────────────
            cur.execute("""
                SELECT id_vendor, nama_vendor, status_vendor, score
                FROM vendor
                WHERE nama_vendor ILIKE %s
                LIMIT 3
            """, (f'%{term}%',))
            for v in cur.fetchall():
                key = f"vendor_{v[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[VENDOR] '{v[1]}' -> id_vendor={v[0]}, "
                        f"status={v[2]}, score={v[3]}"
                    )

            # ── Cari di tabel kontrak ─────────────────────────────────
            cur.execute("""
                SELECT k.id_kontrak, k.judul_kontrak, k.no_dokumen_kontrak,
                       k.direksi_pekerjaan, k.status_kontrak, k.tipe_kontrak,
                       v.nama_vendor
                FROM kontrak k
                LEFT JOIN vendor v ON k.id_vendor = v.id_vendor
                WHERE k.judul_kontrak ILIKE %s
                   OR k.no_dokumen_kontrak ILIKE %s
                   OR k.no_po_pr ILIKE %s
                   OR k.direksi_pekerjaan ILIKE %s
                LIMIT 3
            """, (f'%{term}%', f'%{term}%', f'%{term}%', f'%{term}%'))
            for k in cur.fetchall():
                key = f"kontrak_{k[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[KONTRAK] '{k[1]}' -> id_kontrak={k[0]}, "
                        f"doc={k[2]}, direksi={k[3]}, "
                        f"status={k[4]}, tipe={k[5]}, vendor='{k[6]}'"
                    )

            # ── Cari di tabel tagihan ─────────────────────────────────
            cur.execute("""
                SELECT t.id_tagihan, t.nomor_tagihan, t.status_tagihan,
                       t.nilai_tagihan, k.judul_kontrak
                FROM tagihan t
                LEFT JOIN kontrak k ON t.id_kontrak = k.id_kontrak
                WHERE t.nomor_tagihan ILIKE %s
                LIMIT 3
            """, (f'%{term}%',))
            for t in cur.fetchall():
                key = f"tagihan_{t[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[TAGIHAN] '{t[1]}' -> id_tagihan={t[0]}, "
                        f"status={t[2]}, nilai={t[3]}, kontrak='{t[4]}'"
                    )

            # ── Cari di tabel padi ────────────────────────────────────
            cur.execute("""
                SELECT p.id_padi, p.no_pembelian, p.judul_pembelian,
                       p.nilai, v.nama_vendor
                FROM padi p
                LEFT JOIN vendor v ON p.id_vendor = v.id_vendor
                WHERE p.no_pembelian ILIKE %s
                   OR p.judul_pembelian ILIKE %s
                LIMIT 3
            """, (f'%{term}%', f'%{term}%'))
            for p in cur.fetchall():
                key = f"padi_{p[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[PADI] '{p[2]}' -> id_padi={p[0]}, "
                        f"no_pembelian={p[1]}, nilai={p[3]}, vendor='{p[4]}'"
                    )

        conn.close()

        if context:
            result  = "\n\nKONTEKS ENTITAS YANG DITEMUKAN DI DATABASE:\n"
            result += "(Gunakan informasi ini untuk memahami maksud user tanpa perlu klarifikasi)\n"
            result += "\n".join(context)
            return result

        return ""

    except Exception as e:
        print(f"[ENTITY SEARCH ERROR] {e}")
        return ""

# ─── 7. SQL VALIDATOR ─────────────────────────────────────────────────────────
def validate_sql(sql: str) -> tuple:
    sql_upper = sql.upper().strip()

    dangerous = ["UPDATE", "DELETE", "INSERT", "DROP", "ALTER",
                 "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
    for op in dangerous:
        if re.search(r'\b' + op + r'\b', sql_upper):
            return False, f"Operasi {op} tidak diizinkan."

    if re.search(r'SELECT\s+\*', sql_upper):
        return False, "Query SELECT * tidak diizinkan."

    if not re.search(r'\bSELECT\b', sql_upper):
        return False, "Hanya query SELECT yang diizinkan."

    if "LIMIT" not in sql_upper:
        sql = sql.rstrip(";") + " LIMIT 50"

    return True, sql

# ─── 8. EXECUTE QUERY ─────────────────────────────────────────────────────────
def execute_query(sql: str) -> tuple:
    valid, result = validate_sql(sql)
    if not valid:
        return [], []

    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(result)
            rows    = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            data    = [dict(row) for row in rows]
        conn.close()
        return data, columns
    except Exception as e:
        print(f"[QUERY ERROR] {e}")
        return [], []

# ─── 9. FORMAT DATA TO WA TEXT ────────────────────────────────────────────────
def format_data_for_wa(data: list, columns: list, original_question: str, narrative_hint: str) -> str:
    """Ubah hasil query ke narasi WhatsApp yang readable."""
    if not data:
        return "Tidak ditemukan data yang sesuai. 🔍"

    row_count = len(data)

    # Untuk data sedikit: minta AI buatkan narasi
    if row_count <= 5 and len(columns) <= 6:
        # Serialisasi value datetime
        clean_data = []
        for row in data:
            clean_row = {}
            for k, v in row.items():
                clean_row[k] = v.isoformat() if hasattr(v, 'isoformat') else v
            clean_data.append(clean_row)

        try:
            narrative = call_ai([
                {
                    "role": "system",
                    "content": (
                        "Kamu adalah asisten laporan bisnis via WhatsApp. "
                        "Jawab dalam Bahasa Indonesia yang profesional dan natural. "
                        "Gunakan format WhatsApp: *bold* untuk poin penting, • untuk list, emoji relevan. "
                        "Jangan gunakan tabel HTML. Jika ada nilai uang, format sebagai Rupiah (Rp 1.250.000.000)."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f'Pertanyaan user: "{original_question}"\n'
                        f"Hint narasi: {narrative_hint}\n"
                        f"Data hasil query: {json.dumps(clean_data, default=str, ensure_ascii=False)}\n\n"
                        "Buatkan narasi singkat dalam Bahasa Indonesia yang menjawab pertanyaan tersebut. "
                        "Maksimal 5 kalimat atau poin."
                    )
                }
            ], max_tokens=600)
            return narrative
        except Exception as e:
            print(f"[NARRATIVE ERROR] {e}")
            # Fallback ke format manual
            pass

    # Untuk data banyak: format ringkasan manual
    lines = [f"📊 *Ditemukan {row_count} data*\n"]
    display_data = data[:7]  # Tampilkan max 7 item

    for row in display_data:
        parts = []
        for col in columns[:4]:  # Max 4 kolom per baris agar tidak terlalu panjang
            val = row.get(col)
            if val is None:
                continue
            if hasattr(val, 'isoformat'):
                val = val.strftime('%d/%m/%Y') if hasattr(val, 'strftime') else val.isoformat()
            parts.append(f"{col}: {val}")
        lines.append("• " + " | ".join(parts))

    if row_count > 7:
        lines.append(f"\n_(Menampilkan 7 dari {row_count} data, tanya lebih spesifik untuk detail)_")

    return "\n".join(lines)

# ─── 10. MEMORY PER NOMOR WA ──────────────────────────────────────────────────
MAX_HISTORY    = 10
wa_histories: dict = {}

def get_history(number: str) -> list:
    return wa_histories.get(number, [])

def add_history(number: str, question: str, answer: str):
    history = wa_histories.get(number, [])
    history.append({"role": "user",      "content": question})
    history.append({"role": "assistant", "content": answer})
    if len(history) > MAX_HISTORY * 2:
        history = history[-(MAX_HISTORY * 2):]
    wa_histories[number] = history

def clear_history(number: str):
    wa_histories.pop(number, None)

# ─── 10b. FITUR #LAPORAN ──────────────────────────────────────────────────────

LAPORAN_SYSTEM_PROMPT = (
    "Kamu adalah parser laporan harian maintenance kilang minyak.\n"
    "Tugasmu mengekstrak data dari teks laporan narasi ke dalam format JSON terstruktur.\n\n"
    "DISIPLIN YANG VALID: Electrical, Instrument, Rotating, Stationary, Alat Berat\n\n"
    "KATEGORI YANG VALID:\n"
    "- Corrective Maintenance\n"
    "- Preventive Maintenance\n"
    "- Plant Patrol\n"
    "- Progress\n"
    "- Challenge Session\n"
    "- Support\n\n"
    "STATUS YANG VALID: Done, In Progress, Waiting Material, Pending, -\n"
    "MAPPING STATUS (terapkan tepat seperti ini):\n"
    "  (done), DONE, Done, selesai → Done\n"
    "  (ip), (in progress), In Progress, in progress, sedang dikerjakan → In Progress\n"
    "  waiting material, wm → Waiting Material\n"
    "  pending → Pending\n"
    "  Jika tidak ada keterangan status → -\n\n"
    "DIREKSI (area kerja) YANG VALID: MA5, MA6, MA7, Workshop\n"
    "Normalisasi: 'Maintenance Area 7' / 'Area 7' / 'MA 7' / 'Bagian 7' → 'MA7'\n"
    "             'Maintenance Area 5' / 'Area 5' / 'MA 5' / 'Bagian 5' → 'MA5'\n"
    "             'Maintenance Area 6' / 'Area 6' / 'MA 6' / 'Bagian 6' → 'MA6'\n"
    "             'Workshop' → 'Workshop'\n"
    "Jika tidak ada informasi direksi, gunakan string kosong.\n\n"
    "TAG NUMBER: Kode identifikasi equipment/alat yang biasanya ada di awal deskripsi item,\n"
    "dipisah dengan titik dua (:) atau spasi. Contoh: 101-P-105, 104-P-107, 101A514, 105-FV-020.\n"
    "Format umum: [area]-[tipe]-[nomor] atau [area][kode][nomor].\n"
    "Jika tidak ada tag number, gunakan string kosong.\n\n"
    "ATURAN EKSTRAKSI:\n"
    "1. Satu item pekerjaan = satu entri JSON\n"
    "2. Deteksi tanggal dari teks laporan (format DD/MM/YYYY, DD Bulan YYYY, dsb)\n"
    "3. Deteksi disiplin dari header laporan\n"
    "4. Deteksi direksi dari header laporan, normalisasi ke MA5/MA6/MA7/Workshop\n"
    "5. Petakan setiap item ke kategori yang sesuai\n"
    "6. Ekstrak status menggunakan MAPPING STATUS di atas\n"
    "7. Ekstrak tag number dari awal deskripsi item jika ada\n"
    "8. Deskripsi diisi tanpa tag number (tag number sudah dipisah di field tag_number)\n"
    "9. Catatan: info tambahan yang relevan (target tanggal, detail teknis, dll)\n\n"
    "ATURAN MULTI-TAG (penting):\n"
    "Jika satu baris menyebut beberapa tag sekaligus, buat SATU entri per tag.\n"
    "Contoh: 'Perbaikan fireproofing: 105-P-506, 105-P-508, 105-P-507 (in progress)'\n"
    "→ 3 entri terpisah, masing-masing dengan tag berbeda, deskripsi & status sama.\n\n"
    "ATURAN ABAIKAN BARIS BERIKUT (jangan buat entri JSON):\n"
    "- Baris template/placeholder, contoh: 'Tag Number/Equipment/Func. Loc: Aktifitas (status)', '...', '..'\n"
    "- Baris sub-header equipment, contoh: '* Equipment : Transmitter', '* Equipment : Junction Box'\n"
    "- Baris kosong atau hanya berisi tanda baca\n\n"
    "RESPONSE FORMAT — kembalikan HANYA array JSON, tanpa teks lain:\n"
    '[\n  {\n    "tanggal_laporan": "2026-05-26",\n    "disiplin": "Instrument",\n'
    '    "direksi": "MA7",\n    "kategori": "Plant Patrol",\n    "tag_number": "105-FV-020",\n'
    '    "deskripsi": "Plant Patrol control valve",\n'
    '    "status_pekerjaan": "Done",\n    "catatan": ""\n  }\n]\n\n'
    "PENTING: Kembalikan HANYA array JSON yang valid. Jangan tambahkan penjelasan apapun."
)

# ── Pola untuk filter entri sampah post-parse ──────────────────────────────────
PLACEHOLDER_PATTERNS = [
    r'^tag\s*number',
    r'^func.*loc',
    r'^\.*$',
    r'^aktifitas',
    r'^\s*\.\.\.*\s*$',
]

def is_junk_entry(item: dict) -> bool:
    """Return True kalau entri ini template/placeholder/sampah."""
    deskripsi  = item.get("deskripsi",  "").strip().lower()
    tag_number = item.get("tag_number", "").strip().lower()
    combined   = f"{tag_number} {deskripsi}".strip()
    for pat in PLACEHOLDER_PATTERNS:
        if re.search(pat, combined, re.IGNORECASE):
            return True
    if not deskripsi and not tag_number:
        return True
    return False


def extract_laporan_header(raw_text: str) -> dict:
    """
    Ekstrak header laporan (tanggal, semua disiplin, direksi) secara regex
    tanpa perlu AI — cepat dan hemat token.
    disiplin sekarang berupa list (bisa lebih dari satu).
    """
    header = {"tanggal": "", "disiplin": [], "direksi": ""}

    # ── Tanggal ────────────────────────────────────────────────────────────────
    BULAN_MAP = {
        "januari": "01", "februari": "02", "maret": "03", "april": "04",
        "mei": "05", "juni": "06", "juli": "07", "agustus": "08",
        "september": "09", "oktober": "10", "november": "11", "desember": "12"
    }
    m = re.search(r'\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b', raw_text)
    if m:
        header["tanggal"] = f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    else:
        m = re.search(
            r'\b(\d{1,2})\s+(' + '|'.join(BULAN_MAP.keys()) + r')\s+(\d{4})\b',
            raw_text, re.IGNORECASE
        )
        if m:
            bln = BULAN_MAP[m.group(2).lower()]
            header["tanggal"] = f"{m.group(3)}-{bln}-{m.group(1).zfill(2)}"

    # ── Disiplin — kumpulkan semua yang ada ───────────────────────────────────
    DISIPLIN_LIST = ["Electrical", "Instrument", "Rotating", "Stationary", "Alat Berat"]
    for d in DISIPLIN_LIST:
        if re.search(r'\b' + d + r'\b', raw_text, re.IGNORECASE):
            header["disiplin"].append(d)
    # Tangkap "INSTRUMENTASI" → "Instrument"
    if re.search(r'\bINSTRUMENTASI\b', raw_text, re.IGNORECASE):
        if "Instrument" not in header["disiplin"]:
            header["disiplin"].append("Instrument")

    # ── Direksi / Area ─────────────────────────────────────────────────────────
    DIREKSI_MAP = {
        r'\bMA\s*7\b': "MA7", r'\bMA\s*6\b': "MA6", r'\bMA\s*5\b': "MA5",
        r'[Mm]aintenance\s+[Aa]rea\s*7': "MA7",
        r'[Mm]aintenance\s+[Aa]rea\s*6': "MA6",
        r'[Mm]aintenance\s+[Aa]rea\s*5': "MA5",
        r'\bWorkshop\b': "Workshop",
    }
    for pattern, val in DIREKSI_MAP.items():
        if re.search(pattern, raw_text):
            header["direksi"] = val
            break

    return header


def split_laporan_to_chunks(raw_text: str) -> list:
    """
    Potong teks laporan secara hierarkis: disiplin → kategori.
    Setiap chunk membawa header disiplinnya sendiri sehingga AI
    dapat men-tag disiplin dengan benar.
    Chunk yang terlalu besar (>MAX_ITEMS_PER_CHUNK baris item)
    dipecah otomatis agar tidak melebihi max_tokens output.
    Mengembalikan list of (header_text, chunk_text).
    """
    DISIPLIN_PATTERNS = {
        "Rotating":   r'^ROTATING\s*$',
        "Stationary": r'^STATIONARY\s*$',
        "Electrical": r'^ELECTRICAL\s*$',
        "Instrument": r'^INSTRUMENTASI\s*$|^INSTRUMENT\s*$',
        "Alat Berat": r'^ALAT\s*BERAT\s*$',
    }
    CATEGORY_KEYWORDS = [
        "Plant Patrol", "Preventive Maintenance", "Corrective Maintenance",
        "Progress", "Challenge Session", "Project", "Support",
    ]
    MAX_ITEMS_PER_CHUNK = 25

    lines = raw_text.splitlines()

    # ── Pecah per disiplin ─────────────────────────────────────────────────────
    sections = []
    current_disiplin = ""
    current_lines    = []

    for line in lines:
        stripped = line.strip()
        matched_disiplin = None
        for d, pat in DISIPLIN_PATTERNS.items():
            if re.match(pat, stripped, re.IGNORECASE):
                matched_disiplin = d
                break

        if matched_disiplin:
            if current_lines:
                sections.append((current_disiplin, current_lines))
            current_disiplin = matched_disiplin
            current_lines    = [line]
        else:
            current_lines.append(line)

    if current_lines:
        sections.append((current_disiplin, current_lines))

    # ── Fallback: tidak ada penanda disiplin → logika lama (split per kategori) ─
    has_discipline = any(d for d, _ in sections)
    if not has_discipline:
        cat_pattern = (
            r'(?:(?<=\n)|(?:^))(?='
            + '|'.join(re.escape(k) for k in CATEGORY_KEYWORDS)
            + r')'
        )
        parts = re.split(cat_pattern, raw_text, flags=re.IGNORECASE | re.MULTILINE)
        header_lines, chunks_old = [], []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if any(part.lower().startswith(k.lower()) for k in CATEGORY_KEYWORDS):
                chunks_old.append(part)
            else:
                header_lines.append(part)
        header_text = "\n".join(header_lines)
        if not chunks_old:
            return [(header_text, raw_text)]
        return [(header_text, c) for c in chunks_old]

    # ── Pisahkan header global (baris sebelum disiplin pertama) ───────────────
    global_header = ""
    if sections and not sections[0][0]:
        global_header = "\n".join(sections[0][1]).strip()
        sections = sections[1:]

    # ── Per section disiplin, pecah lagi per kategori ─────────────────────────
    result_chunks = []

    for disiplin, disc_lines in sections:
        disc_text = "\n".join(disc_lines)

        cat_pattern = (
            r'(?:(?<=\n)|(?:^))(?='
            + '|'.join(re.escape(k) for k in CATEGORY_KEYWORDS)
            + r')'
        )
        parts = re.split(cat_pattern, disc_text, flags=re.IGNORECASE | re.MULTILINE)

        disc_header_parts, cat_chunks = [], []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if any(part.lower().startswith(k.lower()) for k in CATEGORY_KEYWORDS):
                cat_chunks.append(part)
            else:
                disc_header_parts.append(part)

        disc_header     = "\n".join(disc_header_parts).strip()
        combined_header = "\n".join(filter(None, [global_header, disc_header]))

        if not cat_chunks:
            result_chunks.append((combined_header, disc_text))
            continue

        for cat_chunk in cat_chunks:
            item_lines = [
                l for l in cat_chunk.splitlines()
                if re.match(r'^\s*[\d\*\-•]', l)
            ]
            if len(item_lines) > MAX_ITEMS_PER_CHUNK:
                cat_header_line = cat_chunk.splitlines()[0]
                for i in range(0, len(item_lines), MAX_ITEMS_PER_CHUNK):
                    batch     = item_lines[i:i + MAX_ITEMS_PER_CHUNK]
                    sub_chunk = cat_header_line + "\n" + "\n".join(batch)
                    result_chunks.append((combined_header, sub_chunk))
            else:
                result_chunks.append((combined_header, cat_chunk))

    return result_chunks


def parse_chunk_with_ai(header_text: str, chunk_text: str) -> list:
    """Parse satu chunk (satu kategori/disiplin) dengan AI."""
    combined = f"{header_text}\n\n{chunk_text}" if header_text else chunk_text
    try:
        response = call_ai([
            {"role": "system", "content": LAPORAN_SYSTEM_PROMPT},
            {"role": "user",   "content": f"Parse laporan berikut:\n\n{combined}"}
        ], max_tokens=3000)

        json_match = re.search(r'\[[\s\S]*\]', response)
        if not json_match:
            print(f"[PARSE CHUNK] Tidak ada JSON: {response[:200]}")
            return []

        parsed = json.loads(json_match.group())
        if not isinstance(parsed, list):
            return []

        # Filter entri sampah/placeholder
        clean = [item for item in parsed if not is_junk_entry(item)]
        filtered_count = len(parsed) - len(clean)
        if filtered_count:
            print(f"[PARSE CHUNK] Dibuang {filtered_count} entri sampah/placeholder")

        return clean

    except Exception as e:
        print(f"[PARSE CHUNK ERROR] {e}")
        return []


def parse_laporan_with_ai(raw_text: str) -> list:
    """
    Parse laporan dengan chunking hierarkis per disiplin → kategori.
    Setiap chunk diparse terpisah lalu digabungkan.
    """
    chunks = split_laporan_to_chunks(raw_text)
    print(f"[PARSE LAPORAN] Total chunks: {len(chunks)}")

    all_items = []
    for i, (header_text, chunk_text) in enumerate(chunks):
        print(f"[PARSE LAPORAN] Memproses chunk {i+1}/{len(chunks)}: "
              f"{chunk_text[:60].strip()!r}...")
        items = parse_chunk_with_ai(header_text, chunk_text)
        print(f"[PARSE LAPORAN] Chunk {i+1} → {len(items)} item")
        all_items.extend(items)

    # Deduplikasi: tanggal + disiplin + tag_number + deskripsi
    seen = set()
    unique_items = []
    for item in all_items:
        key = (
            item.get("tanggal_laporan", ""),
            item.get("disiplin", ""),
            item.get("tag_number", "").strip().lower(),
            item.get("deskripsi", "").strip().lower(),
        )
        if key not in seen:
            seen.add(key)
            unique_items.append(item)

    print(f"[PARSE LAPORAN] Total item unik: {len(unique_items)}")
    return unique_items


def insert_daily_report(items: list, pengirim_wa: str, raw_text: str) -> tuple:
    """
    Simpan items ke daily_report.
    Return: (success_count, error_msg | None, dup_warnings_list, saved_items_list)
    """
    if not items:
        return 0, "Tidak ada item yang bisa diparse", [], []

    # Fallback direksi dari raw_text kalau item tidak punya
    fallback_direksi = ""
    DIREKSI_MAP_FB = {
        r'\bMA\s*7\b': "MA7", r'\bMA\s*6\b': "MA6", r'\bMA\s*5\b': "MA5",
        r'[Mm]aintenance\s+[Aa]rea\s*7': "MA7",
        r'[Mm]aintenance\s+[Aa]rea\s*6': "MA6",
        r'[Mm]aintenance\s+[Aa]rea\s*5': "MA5",
        r'\bWorkshop\b': "Workshop",
    }
    for pat, val in DIREKSI_MAP_FB.items():
        if re.search(pat, raw_text):
            fallback_direksi = val
            break

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        success      = 0
        skipped      = 0
        dup_warnings = []
        saved_items  = []

        for item in items:
            if not item.get("tanggal_laporan") or not item.get("disiplin") or not item.get("deskripsi"):
                skipped += 1
                continue

            # Fallback direksi jika kosong
            direksi = item.get("direksi", "") or fallback_direksi

            # Cek duplikat di DB
            cur.execute("""
                SELECT id_report FROM daily_report
                WHERE tanggal_laporan = %s
                  AND disiplin        = %s
                  AND tag_number      = %s
                  AND deskripsi       = %s
                LIMIT 1
            """, (
                item.get("tanggal_laporan"),
                item.get("disiplin", "-"),
                item.get("tag_number", ""),
                item.get("deskripsi", "-"),
            ))
            if cur.fetchone():
                label = item.get("tag_number") or item.get("deskripsi", "?")[:30]
                dup_warnings.append(label)
                continue

            cur.execute("""
                INSERT INTO daily_report
                    (tanggal_laporan, disiplin, direksi, kategori, tag_number, deskripsi,
                     status_pekerjaan, catatan, pengirim_wa, raw_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item.get("tanggal_laporan"),
                item.get("disiplin",        "-"),
                direksi,
                item.get("kategori",        "-"),
                item.get("tag_number",      ""),
                item.get("deskripsi",       "-"),
                item.get("status_pekerjaan","-"),
                item.get("catatan",         ""),
                pengirim_wa,
                raw_text,
            ))
            saved_items.append(item)
            success += 1

        conn.commit()
        conn.close()
        if skipped:
            print(f"[INSERT LAPORAN] {skipped} item dilewati (field wajib kosong)")
        return success, None, dup_warnings, saved_items

    except Exception as e:
        print(f"[INSERT LAPORAN ERROR] {e}")
        return 0, str(e), [], []


def process_laporan(raw_text: str, sender: str) -> str:
    print(f"[LAPORAN] Memproses dari {sender}, panjang: {len(raw_text)} karakter")

    header   = extract_laporan_header(raw_text)
    chunks   = split_laporan_to_chunks(raw_text)
    n_chunks = len(chunks)

    items = parse_laporan_with_ai(raw_text)
    if not items:
        return (
            "⚠️ *Gagal memparse laporan.*\n\n"
            "Pastikan format laporan sudah benar:\n"
            "• Ada tanggal (contoh: 03/06/2026 atau 3 Juni 2026)\n"
            "• Ada disiplin (Electrical/Instrument/Rotating/Stationary/Alat Berat)\n"
            "• Ada kategori pekerjaan (Plant Patrol / Preventive / Corrective / Project)\n"
            "• Ada daftar item pekerjaan\n\n"
            "Coba kirim ulang dengan format yang lebih jelas."
        )

    success_count, error, dup_warnings, saved_items = insert_daily_report(items, sender, raw_text)
    if error:
        return f"⚠️ *Gagal menyimpan laporan:* {error}"
    if success_count == 0 and not dup_warnings:
        return "⚠️ *Tidak ada data yang berhasil disimpan.* Periksa format laporan."

    # Ringkasan dari saved_items saja (bukan semua items)
    summary = {}
    for item in saved_items:
        key = f"{item.get('disiplin', '-')} — {item.get('kategori', '-')}"
        summary[key] = summary.get(key, 0) + 1
    summary_lines = "\n".join([f"  • {k}: {v} item" for k, v in summary.items()])

    disiplin_str = ", ".join(header["disiplin"]) if header["disiplin"] else "-"
    tgl_info     = f"📅 *Tanggal:* {header['tanggal']}\n"              if header["tanggal"]  else ""
    area_info    = f"🏭 *Area:* {header['direksi']} — {disiplin_str}\n" if header["direksi"]  else ""
    chunk_info   = f"🔀 *Diproses dalam:* {n_chunks} bagian\n"          if n_chunks > 1       else ""

    # Info duplikat
    dup_info = ""
    if dup_warnings:
        dup_list = ", ".join(dup_warnings[:5])
        lebih    = f" (+{len(dup_warnings)-5} lainnya)" if len(dup_warnings) > 5 else ""
        dup_info = f"\n⚠️ *Duplikat dilewati ({len(dup_warnings)} item):* _{dup_list}{lebih}_\n"

    return (
        f"✅ *Laporan berhasil disimpan!*\n\n"
        f"{tgl_info}{area_info}{chunk_info}\n"
        f"📋 *Total:* {success_count} kegiatan tercatat\n"
        f"{dup_info}\n"
        f"*Rincian per kategori:*\n{summary_lines}\n\n"
        f"_Data tersimpan di database dan bisa ditanyakan kapan saja._"
    )

# ─── 11. CORE FUNCTION ────────────────────────────────────────────────────────
def run_wa(question: str, sender: str) -> str:
    # 1. Smart entity search — injeksi konteks dinamis dari DB
    dynamic_context = smart_entity_search(question)
    system_prompt   = BASE_SYSTEM_PROMPT + dynamic_context

    # 2. Build messages dengan history
    history  = get_history(sender)
    messages = [{"role": "system", "content": system_prompt}]
    messages += history[-MAX_HISTORY * 2:]  # Ambil history terakhir
    messages.append({"role": "user", "content": question})

    try:
        raw = call_ai(messages, max_tokens=1500)

        # Coba parse JSON dari respons AI
        json_match = re.search(r'\{[\s\S]*\}', raw)
        if not json_match:
            # Kalau tidak ada JSON, anggap langsung narasi
            answer = raw
            add_history(sender, question, answer)
            return answer

        parsed        = json.loads(json_match.group())
        response_type = parsed.get("type", "narrative")

        # ── CLARIFICATION ──
        if response_type == "clarification":
            answer = parsed.get("clarification_question", parsed.get("message", ""))
            add_history(sender, question, answer)
            return answer

        # ── ERROR ──
        if response_type == "error":
            answer = parsed.get("message", "Maaf, terjadi kesalahan dalam memproses pertanyaan Anda.")
            add_history(sender, question, answer)
            return answer

        # ── NARRATIVE (sapaan, info umum, dsb) ──
        if response_type == "narrative":
            answer = parsed.get("message", raw)
            add_history(sender, question, answer)
            return answer

        # ── QUERY — generate SQL, eksekusi, format ke WA ──
        if response_type == "query" and parsed.get("sql"):
            sql = parsed["sql"]

            valid, val_result = validate_sql(sql)
            if not valid:
                answer = f"⚠️ Maaf, query tidak valid: {val_result}"
                add_history(sender, question, answer)
                return answer

            data, columns = execute_query(val_result)

            if not data:
                answer = "🔍 Tidak ditemukan data yang sesuai dengan pertanyaan Anda."
            else:
                answer = format_data_for_wa(
                    data, columns, question,
                    parsed.get("narrative_hint", "")
                )

            # Tambahkan penjelasan singkat dari AI jika ada
            explanation = parsed.get("explanation", "")
            if explanation and len(data) > 0:
                answer = f"_{explanation}_\n\n" + answer

            add_history(sender, question, answer)
            return answer

        # Fallback
        answer = parsed.get("message", raw)
        add_history(sender, question, answer)
        return answer

    except json.JSONDecodeError:
        answer = raw
        add_history(sender, question, answer)
        return answer
    except Exception as e:
        print(f"[RUN_WA ERROR] {e}")
        return "⚠️ Maaf, terjadi kesalahan sistem. Silakan coba beberapa saat lagi."

# ─── 12. HELPER KIRIM WA ──────────────────────────────────────────────────────
def send_wa(target: str, message: str) -> dict:
    try:
        response = requests.post(
            "https://api.fonnte.com/send",
            headers={"Authorization": FONNTE_TOKEN},
            data={"target": target, "message": message},
            timeout=30,
        )
        return response.json()
    except Exception as e:
        print(f"[SEND WA ERROR] {e}")
        return {}

# ─── 13. FLASK APP ────────────────────────────────────────────────────────────
app = Flask(__name__)

# Set untuk deduplication pesan
processed_messages: set = set()

@app.route("/webhook", methods=["POST"])
def webhook():
    data    = request.get_json(force=True, silent=True) or {}
    sender  = data.get("sender", "")
    message = data.get("message", "").strip()

    # ── Deduplication ─────────────────────────────────────────────────────────
    msg_id = data.get("id") or data.get("message_id") or f"{sender}:{message[:80]}"
    if msg_id in processed_messages:
        print(f"[DUPLIKAT DIABAIKAN] {msg_id}")
        return jsonify({"status": "duplicate"}), 200
    processed_messages.add(msg_id)

    # ── Deteksi pesan dari grup ────────────────────────────────────────────────
    is_group    = data.get("group", False) or (isinstance(sender, str) and "@g.us" in sender)
    participant = data.get("participant", "")
    identity    = participant if is_group and participant else sender

    print(f"[WEBHOOK] sender={sender}, participant={participant}, is_group={is_group}")

    # ── Filter grup: harus pakai trigger ──────────────────────────────────────
    GROUP_TRIGGERS = ["!tanya", "/tanya", "!ai", "/ai", "bot:", "bot :"]
    if is_group:
        message_lower   = message.lower()
        matched_trigger = None
        for trigger in GROUP_TRIGGERS:
            if message_lower.startswith(trigger):
                matched_trigger = trigger
                break

        if not matched_trigger:
            print(f"[GRUP] Diabaikan (tidak ada trigger): {message[:50]}")
            return jsonify({"status": "ignored_no_trigger"}), 200

        # Hapus trigger, ambil pertanyaan saja
        message = message[len(matched_trigger):].strip()
        if not message:
            threading.Thread(
                target=send_wa,
                args=(sender, "❓ Pertanyaanmu kosong.\nContoh: *!tanya berapa kontrak aktif di MA5?*"),
                daemon=True
            ).start()
            return jsonify({"status": "ok"}), 200

    if not message:
        return jsonify({"status": "empty"}), 200

    # ── Command reset history ──────────────────────────────────────────────────
    if message.lower() in ["/reset", "reset", ".reset"]:
        clear_history(identity)
        threading.Thread(
            target=send_wa,
            args=(sender, "🔄 *Percakapan direset.* Memori sesi sebelumnya dihapus."),
            daemon=True
        ).start()
        return jsonify({"status": "ok"}), 200

    # ── Deteksi #laporan ──────────────────────────────────────────────────────
    LAPORAN_TRIGGERS = ["#laporan", "#report", "#lpr"]
    matched_laporan = None
    for trigger in LAPORAN_TRIGGERS:
        if message.lower().startswith(trigger):
            matched_laporan = trigger
            break

    if matched_laporan:
        laporan_text = message[len(matched_laporan):].strip()
        if not laporan_text:
            threading.Thread(
                target=send_wa,
                args=(sender, (
                    "📋 *Format pengiriman laporan:*\n\n"
                    "*#laporan* [isi laporan]\n\n"
                    "Contoh:\n"
                    "#laporan Pekerjaan Electrical 26 Mei 2026\n"
                    "Corrective Maintenance\n"
                    "1. Perbaikan soot blower (Done)\n"
                    "..."
                )),
                daemon=True
            ).start()
            return jsonify({"status": "ok"}), 200

        def process_lap():
            answer = process_laporan(laporan_text, identity)
            send_wa(sender, answer)

        threading.Thread(target=process_lap, daemon=True).start()
        return jsonify({"status": "ok"}), 200

    # ── Proses di background thread ───────────────────────────────────────────
    # Reply 200 OK dulu ke Fonnte agar tidak timeout & tidak kirim ulang
    def process():
        try:
            answer = run_wa(message, identity)
            send_wa(sender, answer)
        except Exception as e:
            print(f"[ERROR] Gagal proses pesan dari {identity}: {e}")
            send_wa(sender, "⚠️ Maaf, terjadi kesalahan. Silakan coba beberapa saat lagi.")

    threading.Thread(target=process, daemon=True).start()
    return jsonify({"status": "ok"}), 200

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "Kontrak WA Bot is running 🚀"}), 200

# ─── 14. RUN ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"🚀 Kontrak WA Bot berjalan di port {port}...")
    app.run(host="0.0.0.0", port=port)