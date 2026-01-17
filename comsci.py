import sqlite3
import datetime

DB_NAME = "petrol_auto_invoice.db"

# ---------------- DATABASE INITIALIZATION ----------------

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")

    c.execute("""
    CREATE TABLE IF NOT EXISTS customer_orders (
        order_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        petrol_type TEXT,
        qty_ordered REAL,
        transaction_date TEXT
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS collection_invoices (
        collection_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        collection_date TEXT,
        total_qty_collected REAL,
        petrol_type TEXT,
        status TEXT DEFAULT 'PENDING'
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS withdrawals (
        withdrawal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_id INTEGER,
        order_id INTEGER,
        qty_taken REAL,
        FOREIGN KEY (collection_id) REFERENCES collection_invoices(collection_id),
        FOREIGN KEY (order_id) REFERENCES customer_orders(order_id)
    );
    """)

    conn.commit()
    return conn

# ---------------- DATABASE MIGRATION (HL SAFE FIX) ----------------

def migrate_db(conn):
    c = conn.cursor()
    c.execute("PRAGMA table_info(collection_invoices);")
    columns = [col[1] for col in c.fetchall()]

    if "status" not in columns:
        c.execute("""
            ALTER TABLE collection_invoices
            ADD COLUMN status TEXT DEFAULT 'PENDING'
        """)
        conn.commit()

# ---------------- UTIL ----------------

def get_date_input(prompt):
    date_str = input(f"{prompt} (YYYY-MM-DD) [Enter for Today]: ")
    if not date_str.strip():
        return datetime.date.today().strftime("%Y-%m-%d")
    return date_str

# ---------------- PURCHASE ----------------

def create_transaction(conn):
    print("\n--- NEW PETROL PURCHASE ---")
    try:
        cust_id = int(input("Customer ID: "))
        petrol = input("Petrol Type (DIESEL / 95): ").upper()
        qty = float(input("Quantity Purchased (L): "))
        date = get_date_input("Purchase Date")

        c = conn.cursor()
        c.execute("""
            INSERT INTO customer_orders
            (customer_id, petrol_type, qty_ordered, transaction_date)
            VALUES (?, ?, ?, ?)
        """, (cust_id, petrol, qty, date))

        conn.commit()
        print(f"✅ Purchase recorded. Order ID: {c.lastrowid}")

    except ValueError:
        print("❌ Invalid input.")

# ---------------- COLLECTION REQUEST ----------------

def withdraw_petrol(conn):
    print("\n--- REQUEST PETROL COLLECTION ---")
    try:
        cust_id = int(input("Customer ID: "))
        petrol = input("Petrol Type: ").upper()
    except ValueError:
        return

    c = conn.cursor()
    c.execute("""
        SELECT o.order_id, o.qty_ordered, COALESCE(SUM(w.qty_taken),0)
        FROM customer_orders o
        LEFT JOIN withdrawals w ON o.order_id = w.order_id
        WHERE o.customer_id = ? AND o.petrol_type = ?
        GROUP BY o.order_id
        ORDER BY o.order_id
    """, (cust_id, petrol))

    orders = c.fetchall()
    available = sum(o[1] - o[2] for o in orders if (o[1] - o[2]) > 0)

    print(f"Available {petrol}: {available} L")

    if available <= 0:
        print("❌ No petrol available.")
        return

    try:
        qty = float(input("Quantity to collect: "))
        if qty > available:
            print("❌ Requested amount exceeds balance.")
            return

        date = get_date_input("Collection Date")

        c.execute("""
            INSERT INTO collection_invoices
            (customer_id, collection_date, total_qty_collected, petrol_type)
            VALUES (?, ?, ?, ?)
        """, (cust_id, date, qty, petrol))

        invoice_id = c.lastrowid

        qty_left = qty
        for o in orders:
            if qty_left <= 0:
                break
            oid, bought, used = o
            remaining = bought - used
            if remaining > 0:
                take = min(remaining, qty_left)
                c.execute("""
                    INSERT INTO withdrawals
                    (collection_id, order_id, qty_taken)
                    VALUES (?, ?, ?)
                """, (invoice_id, oid, take))
                qty_left -= take

        conn.commit()

        print("\n✅ COLLECTION REQUEST CREATED")
        print(f"📄 Invoice ID: {invoice_id}")
        print("⚠️ Take this Invoice ID to Warehouse B for verification.")

    except ValueError:
        print("❌ Invalid input.")

# ---------------- WAREHOUSE VERIFICATION ----------------

def verify_invoice_at_warehouse(conn):
    print("\n--- WAREHOUSE VERIFICATION ---")
    try:
        invoice_id = int(input("Invoice ID: "))
        cust_id = int(input("Customer ID: "))
    except ValueError:
        print("❌ Invalid input.")
        return

    c = conn.cursor()
    c.execute("""
        SELECT customer_id, petrol_type, total_qty_collected, status
        FROM collection_invoices
        WHERE collection_id = ?
    """, (invoice_id,))

    row = c.fetchone()

    if not row:
        print("❌ Invoice not found.")
        return

    inv_cust, petrol, qty, status = row

    if status == "COLLECTED":
        print("❌ Petrol already collected.")
        return

    if inv_cust != cust_id:
        print("❌ Customer mismatch.")
        return

    print("\n✅ INVOICE VERIFIED")
    print(f"Petrol Type: {petrol}")
    print(f"Authorized Quantity: {qty} L")

    confirm = input("Confirm release? (Y/N): ").upper()
    if confirm == 'Y':
        c.execute("""
            UPDATE collection_invoices
            SET status = 'COLLECTED'
            WHERE collection_id = ?
        """, (invoice_id,))
        conn.commit()
        print("🚛 Petrol released from warehouse.")
    else:
        print("❌ Collection cancelled.")

# ---------------- LEDGER (WITH RUNNING BALANCE) ----------------

def detailed_summary(conn):
    try:
        cust_id = int(input("\nCustomer ID: "))
        petrol = input("Petrol Type (DIESEL / 95): ").upper()
    except ValueError:
        return

    c = conn.cursor()

    print("\n--- CUSTOMER LEDGER ---")
    print(f"{'Date':<12} {'Type':<10} {'Petrol':<8} {'Ref':<15} {'Qty(L)':<10} {'Balance(L)':<12}")
    print("-" * 80)

    c.execute("""
        SELECT transaction_date, 'PURCHASE', petrol_type,
               'ORD#' || order_id, qty_ordered
        FROM customer_orders
        WHERE customer_id = ? AND petrol_type = ?
    """, (cust_id, petrol))

    purchases = c.fetchall()

    c.execute("""
        SELECT collection_date, 'COLLECT', petrol_type,
               'INV#' || collection_id, total_qty_collected
        FROM collection_invoices
        WHERE customer_id = ? AND petrol_type = ?
    """, (cust_id, petrol))

    collections = c.fetchall()

    ledger = sorted(purchases + collections, key=lambda x: x[0])

    balance = 0
    for date, ttype, ptype, ref, qty in ledger:
        if ttype == "PURCHASE":
            balance += qty
        else:
            balance -= qty

        print(f"{date:<12} {ttype:<10} {ptype:<8} {ref:<15} {qty:<10} {balance:<12}")

    print("-" * 80)
    print(f"✅ CURRENT AVAILABLE {petrol}: {balance} L")

# ---------------- MENU ----------------

def main():
    conn = init_db()
    migrate_db(conn)

    while True:
        print("\n=== PETROL AUTO-INVOICE SYSTEM ===")
        print("1. Buy Petrol")
        print("2. Request Collection (Generate Invoice)")
        print("3. Warehouse Verify Invoice")
        print("4. View Customer Ledger")
        print("5. Exit")

        choice = input("Select: ")

        if choice == '1':
            create_transaction(conn)
        elif choice == '2':
            withdraw_petrol(conn)
        elif choice == '3':
            verify_invoice_at_warehouse(conn)
        elif choice == '4':
            detailed_summary(conn)
        elif choice == '5':
            break

if __name__ == "__main__":
    main()


