# ============================================
#   PTL SERVER — Flask + MQTT + MySQL
#   Pengganti Google Apps Script
#   Jalankan: python server.py
# ============================================

from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import paho.mqtt.client as mqtt
import json
import threading
import time

app = Flask(__name__)
CORS(app)

# ── KONFIGURASI DATABASE ─────────────────────
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'ptl_db'
}

# ── KONFIGURASI MQTT ──────────────────────────
MQTT_BROKER = 'localhost'
MQTT_PORT   = 1883

# Overhead header MQTT PUBLISH (QoS 0) sesuai spesifikasi MQTT v3.1.1:
#   2 byte fixed header + 2 byte panjang topic + len("ptl/order")=9 byte = 13 byte
MQTT_HEADER_BYTES = 13

# ── MQTT CLIENT ──────────────────────────────
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def connect_mqtt():
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        print('[MQTT] Terhubung ke broker!')
    except Exception as e:
        print(f'[MQTT] Gagal konek: {e}')

# ── DATABASE HELPER ───────────────────────────
def get_db():
    return mysql.connector.connect(**DB_CONFIG)

# ✅ MAPPING BARU — 16 barang 8 PTL
def get_rack_ptl(rack_id):
    if rack_id in ['A-01','A-02']: return 'PTL 1'
    if rack_id in ['A-03','A-04']: return 'PTL 2'
    if rack_id in ['B-01','B-02']: return 'PTL 3'
    if rack_id in ['B-03','B-04']: return 'PTL 4'
    if rack_id in ['C-01','C-02']: return 'PTL 5'
    if rack_id in ['C-03','C-04']: return 'PTL 6'
    if rack_id in ['D-01','D-02']: return 'PTL 7'
    if rack_id in ['D-03','D-04']: return 'PTL 8'
    return ''

# ============================================
#   API ENDPOINTS
# ============================================

# ✅ DIUPDATE — return date_added + sort by tanggal (FIFO)
@app.route('/api/stok', methods=['GET'])
def get_stok():
    try:
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute("""
            SELECT *,
            DATEDIFF(NOW(), date_added) as hari_tersimpan
            FROM item_data
            ORDER BY rack_id ASC
        """)
        data = cursor.fetchall()
        for item in data:
            item['ptl'] = get_rack_ptl(item['rack_id'])
            if item['date_added']:
                item['date_added'] = str(item['date_added'])
        cursor.close(); db.close()
        return jsonify({'status': 'ok', 'data': data})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/order', methods=['GET'])
def get_order():
    try:
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute("""
            SELECT order_id, rack_id, item_name, qty as qty_to_put, status
            FROM orders WHERE status = 'active'
        """)
        data = cursor.fetchall()
        cursor.close(); db.close()
        return jsonify({'status': 'ok', 'data': data})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/orders/all', methods=['GET'])
def get_all_orders():
    try:
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute("""
            SELECT order_id, rack_id, item_name, qty, status, created_at
            FROM orders ORDER BY id DESC LIMIT 50
        """)
        data = cursor.fetchall()
        for item in data:
            item['ptl'] = get_rack_ptl(item['rack_id'])
            if item['created_at']:
                item['created_at'] = str(item['created_at'])
        cursor.close(); db.close()
        return jsonify({'status': 'ok', 'data': data})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/order/add', methods=['POST'])
def add_order():
    try:
        items = request.json.get('items', [])
        db = get_db()
        cursor = db.cursor()

        cursor.execute("SELECT COUNT(*) FROM orders")
        count = cursor.fetchone()[0]

        order_ids = []
        group_id = f"order_{str(count + 1).zfill(2)}"

        for i, item in enumerate(items):
            if i == 0:
                order_id = group_id
            else:
                cursor.execute("SELECT COUNT(*) FROM orders")
                c = cursor.fetchone()[0]
                order_id = f"order_{str(c + 1).zfill(2)}"

            cursor.execute("""
                INSERT INTO orders (order_id, rack_id, item_name, qty, status)
                VALUES (%s, %s, %s, %s, 'waiting')
            """, (order_id, item['rack_id'], item['item_name'], item['qty']))
            order_ids.append(order_id)

        db.commit()
        cursor.close(); db.close()

        return jsonify({
            'status': 'ok',
            'order_id': group_id,
            'order_ids': order_ids
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/order/activate', methods=['GET', 'POST'])
def activate_order():
    try:
        if request.method == 'GET':
            import json as json_lib
            order_ids_raw = request.args.get('order_ids', '[]')
            order_ids = json_lib.loads(order_ids_raw)
        else:
            order_ids = request.json.get('order_ids', [])

        db = get_db()
        cursor = db.cursor(dictionary=True)
        activated = []

        ptl_groups = {}
        for order_id in order_ids:
            cursor.execute("""
                SELECT * FROM orders WHERE order_id = %s AND status = 'waiting'
            """, (order_id,))
            order = cursor.fetchone()
            if order:
                cursor.execute("""
                    UPDATE orders SET status = 'active' WHERE order_id = %s
                """, (order_id,))
                ptl = get_rack_ptl(order['rack_id'])
                if ptl not in ptl_groups:
                    ptl_groups[ptl] = []
                ptl_groups[ptl].append({
                    'order_id': order_id,
                    'rack_id': order['rack_id'],
                    'item_name': order['item_name'],
                    'qty_to_put': order['qty']
                })
                activated.append(order_id)

        for ptl, ptl_items in ptl_groups.items():
            for item in ptl_items:
                mqtt_payload = json.dumps(item)
                payload_size_bytes = len(mqtt_payload.encode('utf-8'))
                payload_size_bits = payload_size_bytes * 8
                print(f'[SIZE] MQTT payload: {payload_size_bytes} bytes ({payload_size_bits} bits)')
                print(f'[SIZE] Payload content: {mqtt_payload}')
                mqtt_client.publish('ptl/order', mqtt_payload)
                print(f'[MQTT] Publish: {item["order_id"]} → rak {item["rack_id"]} ({ptl})')

                # ✅ BARU — simpan ukuran payload ke MySQL (buat Bab 4)
                try:
                    cursor.execute("""
                        INSERT INTO payload_log
                        (order_id, protocol, topic_endpoint, payload_content,
                         payload_size_bytes, payload_size_bits, header_overhead_bytes, total_size_bytes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (item['order_id'], 'MQTT Lokal', 'ptl/order', mqtt_payload,
                          payload_size_bytes, payload_size_bits,
                          MQTT_HEADER_BYTES, payload_size_bytes + MQTT_HEADER_BYTES))
                except Exception as e:
                    print(f'[SIZE] Gagal simpan payload_log: {e}')

        db.commit()
        cursor.close(); db.close()

        if request.method == 'GET':
            html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
            <meta name="viewport" content="width=device-width,initial-scale=1">
            <title>PTL Aktivasi</title>
            <style>body{{font-family:Arial,sans-serif;text-align:center;padding:40px;background:#f0f2f5}}
            .card{{background:white;border-radius:16px;padding:32px;max-width:400px;margin:0 auto;box-shadow:0 4px 20px rgba(0,0,0,.1)}}
            .icon{{font-size:64px;margin-bottom:16px}}h2{{color:#22c55e}}p{{color:#6b7280}}
            .badge{{background:#dcfce7;color:#16a34a;padding:10px 20px;border-radius:10px;font-weight:bold;display:inline-block;margin-top:16px}}</style>
            </head><body><div class="card"><div class="icon">✅</div>
            <h2>Order Diaktifkan!</h2>
            <p>{len(activated)} order berhasil diaktifkan</p>
            <div class="badge">LED PTL akan menyala!</div>
            <p style="margin-top:16px;font-size:12px;color:#9ca3af">Silakan ambil barang sesuai LED yang menyala</p>
            </div></body></html>"""
            from flask import make_response
            return make_response(html, 200)

        return jsonify({
            'status': 'ok',
            'activated': len(activated),
            'order_ids': activated
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/order/confirm', methods=['GET', 'POST'])
def confirm_pick():
    try:
        if request.method == 'GET':
            order_id   = request.args.get('order_id')
            respond_du = float(request.args.get('respond_du', 0))
        else:
            data       = request.json
            order_id   = data.get('order_id')
            respond_du = float(data.get('respond_du', 0))

        db = get_db()
        cursor = db.cursor(dictionary=True)

        cursor.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
        order = cursor.fetchone()
        if not order:
            return jsonify({'status': 'error', 'message': 'Order tidak ditemukan'})

        cursor.execute("UPDATE orders SET status = 'Done' WHERE order_id = %s", (order_id,))

        cursor.execute("""
            UPDATE item_data SET quantity = quantity - %s
            WHERE rack_id = %s AND item_name = %s
        """, (order['qty'], order['rack_id'], order['item_name']))

        cursor.execute("""
            INSERT INTO picking_log (order_id, rack_id, item_name, qty, respond_du)
            VALUES (%s, %s, %s, %s, %s)
        """, (order_id, order['rack_id'], order['item_name'], order['qty'], respond_du))

        db.commit()
        cursor.close(); db.close()

        return jsonify({'status': 'ok', 'message': 'Pick berhasil dikonfirmasi'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/log/update', methods=['GET'])
def update_log():
    try:
        order_id   = request.args.get('order_id')
        confirm_du = float(request.args.get('confirm_du', 0))
        total_dur  = float(request.args.get('total_dur', 0))

        db = get_db()
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            UPDATE picking_log SET confirm_du = %s, total_dur = %s
            WHERE order_id = %s
            ORDER BY id DESC LIMIT 1
        """, (confirm_du, total_dur, order_id))

        cursor.execute("""
            SELECT * FROM picking_log WHERE order_id = %s
            ORDER BY id DESC LIMIT 1
        """, (order_id,))
        log = cursor.fetchone()

        cursor.execute("""
            SELECT created_at FROM orders WHERE order_id = %s LIMIT 1
        """, (order_id,))
        order = cursor.fetchone()

        if log and log['confirm_du'] > 0 and log['total_dur'] > 0:
            respond_ms = log['respond_du'] * 1000
            confirm_ms = log['confirm_du'] * 1000
            total_ms   = log['total_dur'] * 1000

            latency_order_ms = None
            if order and order['created_at'] and log['created_at']:
                diff = log['created_at'] - order['created_at']
                latency_order_ms = diff.total_seconds() * 1000

            # ✅ BARU — ambil ukuran payload dari payload_log, hitung throughput
            payload_bytes = payload_bits = throughput_bps = None
            try:
                cursor.execute("""
                    SELECT payload_size_bytes, payload_size_bits, total_size_bytes
                    FROM payload_log
                    WHERE order_id = %s AND topic_endpoint = 'ptl/order'
                    ORDER BY id DESC LIMIT 1
                """, (order_id,))
                pl = cursor.fetchone()
                if pl:
                    payload_bytes = pl['payload_size_bytes']
                    payload_bits  = pl['payload_size_bits']
                    if confirm_ms > 0:
                        throughput_bps = round(payload_bits / (confirm_ms / 1000), 2)
            except Exception as e:
                print(f'[PERF] Gagal ambil payload_log: {e}')

            cursor.execute("""
                INSERT INTO performance_test
                (test_name, order_id, rack_id, item_name, latency_order_to_led_ms,
                 latency_led_to_button_ms, latency_button_to_confirm_ms, latency_total_ms,
                 payload_size_bytes, payload_size_bits, throughput_bps, status, tested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Done', %s)
            """, ('CP3 - MySQL + MQTT', log['order_id'], log['rack_id'], log['item_name'],
                  latency_order_ms, respond_ms, confirm_ms, total_ms,
                  payload_bytes, payload_bits, throughput_bps, log['created_at']))

            cursor.execute("""
                INSERT INTO delay_test
                (test_name, order_id, rack_id, item_name, delay_order_to_activate_ms,
                 delay_led_to_button_ms, delay_button_to_server_ms, delay_total_ms, tested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, ('CP3 - MySQL + MQTT', log['order_id'], log['rack_id'], log['item_name'],
                  latency_order_ms, respond_ms, confirm_ms, total_ms, log['created_at']))

            thr_txt = f' payload:{payload_bytes}B thr:{throughput_bps}bps' if payload_bytes else ''
            print(f'[PERF] {order_id} -> respond:{respond_ms:.0f}ms confirm:{confirm_ms:.0f}ms total:{total_ms:.0f}ms{thr_txt}')

        db.commit()
        cursor.close(); db.close()

        return jsonify({'status': 'ok', 'message': 'Log updated'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/log', methods=['GET'])
def get_log():
    try:
        db = get_db()
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM picking_log ORDER BY id DESC LIMIT 50")
        data = cursor.fetchall()
        for item in data:
            if item['created_at']:
                item['created_at'] = str(item['created_at'])
        cursor.close(); db.close()
        return jsonify({'status': 'ok', 'data': data})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# ✅ BARU — Tambah barang baru ke inventory
@app.route('/api/stok/add', methods=['POST'])
def add_stok():
    try:
        data      = request.json
        rack_id   = data.get('rack_id')
        item_name = data.get('item_name')
        quantity  = data.get('quantity', 0)

        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO item_data (rack_id, item_name, quantity, date_added)
            VALUES (%s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
            quantity = quantity + VALUES(quantity),
            date_added = NOW()
        """, (rack_id, item_name, quantity))
        db.commit()
        cursor.close(); db.close()
        return jsonify({'status': 'ok', 'message': 'Stok berhasil ditambahkan'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# ✅ BARU — Update stok existing (restock)
@app.route('/api/stok/update', methods=['POST'])
def update_stok():
    try:
        data     = request.json
        rack_id  = data.get('rack_id')
        quantity = data.get('quantity', 0)

        db = get_db()
        cursor = db.cursor()
        cursor.execute("""
            UPDATE item_data
            SET quantity = %s, date_added = NOW()
            WHERE rack_id = %s
        """, (quantity, rack_id))
        db.commit()
        cursor.close(); db.close()
        return jsonify({'status': 'ok', 'message': 'Stok berhasil diupdate'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/order/cancel', methods=['POST'])
def cancel_order():
    try:
        order_ids = request.json.get('order_ids', [])
        db = get_db()
        cursor = db.cursor()
        
        for order_id in order_ids:
            cursor.execute("""
                UPDATE orders SET status = 'cancelled' 
                WHERE order_id = %s AND status IN ('waiting', 'active')
            """, (order_id,))
        
        # Publish MQTT buat kasih tau ESP reset PTL
        cancel_payload = json.dumps({'action': 'cancel', 'order_ids': order_ids})
        cancel_size_bytes = len(cancel_payload.encode('utf-8'))
        cancel_size_bits = cancel_size_bytes * 8
        print(f'[SIZE] Cancel payload: {cancel_size_bytes} bytes ({cancel_size_bits} bits)')

        # ✅ BARU — simpan ukuran payload cancel ke MySQL
        cancel_header = 2 + 2 + len('ptl/cancel')   # = 14 byte
        try:
            cursor.execute("""
                INSERT INTO payload_log
                (order_id, protocol, topic_endpoint, payload_content,
                 payload_size_bytes, payload_size_bits, header_overhead_bytes, total_size_bytes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (','.join(order_ids), 'MQTT Lokal', 'ptl/cancel', cancel_payload,
                  cancel_size_bytes, cancel_size_bits,
                  cancel_header, cancel_size_bytes + cancel_header))
        except Exception as e:
            print(f'[SIZE] Gagal simpan payload_log: {e}')

        db.commit()
        cursor.close(); db.close()

        mqtt_client.publish('ptl/cancel', cancel_payload)
        print(f'[CANCEL] Order dibatalkan: {order_ids}')
        
        return jsonify({'status': 'ok', 'message': f'{len(order_ids)} order dibatalkan'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/ping', methods=['GET'])
def ping():
    return jsonify({'status': 'ok', 'message': 'PTL Server berjalan!'})

# ============================================
#   MAIN
# ============================================
if __name__ == '__main__':
    print('=' * 50)
    print('  PTL SERVER — Flask + MQTT + MySQL')
    print('  >>> VERSI: payload_log v2 (revisi dospem) <<<')
    print('=' * 50)
    connect_mqtt()
    print('[SERVER] Berjalan di http://localhost:5000')
    print('[SERVER] phpMyAdmin: http://localhost/phpmyadmin')
    print('=' * 50)
    app.run(host='0.0.0.0', port=5000, debug=True)