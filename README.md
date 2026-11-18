# TFEX & BOT Stateless EOD Data Collector

Stateless Python EOD Data Collector สำหรับดึงข้อมูลสิ้นวัน (End-of-Day: EOD) ของตลาด TFEX และอัตราดอกเบี้ย THOR จากธนาคารแห่งประเทศไทย (BOT) บันทึกลง **Turso Cloud SQLite** อย่างเป็นระบบและ Idempotent ผ่าน HTTP Pipeline API (`/v2/pipeline`)

---

## 1. ข้อมูลทั้ง 6 ส่วนที่จัดเก็บ (Database Schema)

| ตาราง | แหล่งข้อมูล | Primary Key | คำอธิบาย |
|---|---|---|---|
| `eod_investor_types` | TFEX Investor Types (HTML) | `(trade_date, category)` | ปริมาณซื้อขายสุทธิแยกประเภทนักลงทุน (สถาบัน, ต่างชาติ, รายย่อย) ของ Futures, Call Options, Put Options |
| `eod_market_stats` | TFEX Market Stats (JSON API) | `(trade_date, market_id, instrument_id)` | สถิติภาพรวมตลาดย้อนหลัง 7 วัน (`TXI_F`, `TXI_C`, `TXI_P`) |
| `eod_futures_series` | TFEX Series Trading (JSON API) | `(trade_date, series)` | ข้อมูลซื้อขาย 5 วันล่าสุดของ 2 Active Quarterly Series (เช่น `S50Z26`, `S50H27`) |
| `eod_options_series` | TFEX Options Series (JSON API) | `(trade_date, symbol)` | ข้อมูลซื้อขาย 5 วันล่าสุดของทุกสัญญา Options ของ 2 Active Series (ดึงคู่ขนานด้วย 8 Workers) |
| `eod_thor_rates` | BOT THOR Rates (HTML Form) | `(trade_date)` | อัตราดอกเบี้ย THOR ย้อนหลัง 30 วัน (`thor_rate`, `thor_1m_avg`, `thor_3m_avg`, `thor_6m_avg`) |
| `eod_options_greeks` | Black-76 Greeks Engine (`greeks_engine.py`) | `(trade_date, symbol)` | ค่ากรีกครบถ้วน First & Second Order (`iv`, `delta`, `gamma`, `theta`, `vega`, `vanna`, `charm`) คำนวณจากราคา Settlement/Close และดอกเบี้ย THOR |

> **หมายเหตุเรื่องความถูกต้องของวันที่ (Date Invariant):** ข้อมูลทุกตารางใช้ `trade_date` จริงที่ติดมากับข้อมูล (แปลงเป็น `YYYY-MM-DD`) ห้ามใช้วันที่ของนาฬิกาเครื่อง เพื่อรองรับรอบการรันข้ามคืน (ตี 1 และ 6 โมงเช้า)

---

## 2. วิธีตั้งค่า GitHub Secrets

ในการรันอัตโนมัติบน GitHub Actions ให้เข้าไปที่ Repository ของคุณบน GitHub:
1. ไปที่ **Settings** > **Secrets and variables** > **Actions**
2. คลิก **New repository secret** และเพิ่ม 2 ค่าดังนี้:

| Secret Name | Value Example | คำอธิบาย |
|---|---|---|
| `TURSO_DATABASE_URL` | `libsql://your-db-org.turso.io` | URL ฐานข้อมูล Turso (สคริปต์จะแปลงเป็น `https://.../v2/pipeline` อัตโนมัติ) |
| `TURSO_AUTH_TOKEN` | `eyJhbGciOi...` | JWT Auth Token ที่ได้จากคำสั่ง `turso db tokens create <db-name>` |
| `TELEGRAM_BOT_TOKEN` | `9267903435:AAE...` | Bot Token สำหรับส่งแจ้งเตือนสถานะ EOD เข้า Telegram |
| `TELEGRAM_CHAT_ID` | `1427208361` | Telegram Chat ID ผู้รับการแจ้งเตือน |

---

## 3. รอบการทำงานอัตโนมัติ (GitHub Actions Schedule)

Workflow ถูกตั้งค่าไว้ใน [`.github/workflows/collect_eod.yml`](.github/workflows/collect_eod.yml) รันวันจันทร์–ศุกร์ตามเวลาประเทศไทย:

- `0 14 * * 1-5` : **21:00 น. วัน T** (สรุปข้อมูลรอบหัวค่ำหลังตลาดปิด)
- `0 18 * * 1-5` : **01:00 น. วัน T+1** (อัปเดตข้อมูลรอบดึกหลัง SET/TFEX ปรับปรุงระบบ EOD)
- `15 23 * * 0-4` : **06:15 น. วันจันทร์–ศุกร์** (ตรวจสอบความครบถ้วนรอบสุดท้ายและส่งแจ้งเตือน Telegram ก่อนตลาดเปิด)

### การสั่งรันแบบ Manual (Workflow Dispatch):
สามารถกด **Run workflow** ได้จากแท็บ Actions บน GitHub โดยมีตัวเลือก:
- `force_run` (boolean): บังคับรันทันทีโดยไม่สนใจวันหยุดเสาร์-อาทิตย์ หรือวันหยุดนักขัตฤกษ์ของตลาดหลักทรัพย์ฯ (`SET_HOLIDAYS`)

---

## 4. วิธีติดตั้งและทดสอบในเครื่อง (Local Testing)

### 4.1 ติดตั้ง Dependencies
```bash
python -m pip install -r requirements.txt
```

### 4.2 ทดสอบด้วย Local SQLite (ไม่ต้องมี Turso Token)
สคริปต์รองรับโหมด Local SQLite เมื่อไม่พบคีย์ Turso เพื่อความสะดวกในการทดสอบ:
```bash
# ทดสอบรันและบันทึกลง local SQLite (eod_tfex.db)
python eod_collector.py --force --local-db eod_tfex.db
```

### 4.3 ทดสอบด้วย Turso Cloud จริง และส่งแจ้งเตือน Telegram
1. คัดลอกไฟล์ `.env.example` เป็น `.env`:
   ```bash
   cp .env.example .env
   ```
2. แก้ไขค่า `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` ใน `.env`
3. รันสคริปต์และทดสอบส่ง Telegram Alert ทันที:
   ```bash
   python eod_collector.py --notify
   ```
