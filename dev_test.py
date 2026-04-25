"""
Dev test: verifica Tor proxy, PostgreSQL local, y rotación de circuito.
Uso: python dev_test.py
"""
import os
import socket
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv(".env.dev")

TOR_PROXY = os.getenv("TOR_PROXY", "socks5h://127.0.0.1:9050")
TOR_CONTROL_HOST = "127.0.0.1"
TOR_CONTROL_PORT = int(os.getenv("TOR_CONTROL_PORT", 9051))
DATABASE_URL = os.getenv("DATABASE_URL")

PROXIES = {"http": TOR_PROXY, "https": TOR_PROXY}


def get_exit_ip(session=None):
    r = (session or requests).get(
        "https://check.torproject.org/api/ip",
        proxies=PROXIES,
        timeout=15,
    )
    return r.json()


def renew_tor_circuit():
    """Envía SIGNAL NEWNYM al ControlPort para rotar el exit node."""
    with socket.create_connection((TOR_CONTROL_HOST, TOR_CONTROL_PORT), timeout=5) as s:
        s.sendall(b'AUTHENTICATE ""\r\nSIGNAL NEWNYM\r\nQUIT\r\n')
        resp = s.recv(1024).decode()
    return "250" in resp


def test_tor():
    print("\n=== TEST TOR ===")
    session = requests.Session()
    session.proxies = PROXIES

    info1 = get_exit_ip(session)
    print(f"  IP inicial : {info1['IP']} | IsTor={info1['IsTor']}")

    print("  Rotando circuito...")
    ok = renew_tor_circuit()
    time.sleep(3)  # Tor necesita ~2s para establecer nuevo circuito

    info2 = get_exit_ip(session)
    print(f"  IP tras rotate: {info2['IP']} | IsTor={info2['IsTor']}")
    print(f"  IPs distintas: {info1['IP'] != info2['IP']}")

    # Test contra exchange real sin API key (endpoint público)
    r = session.get(
        "https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT",
        timeout=15,
    )
    data = r.json()
    price = data["result"]["list"][0]["lastPrice"] if data.get("result") else "N/A"
    print(f"  Bybit BTC/USDT via Tor: ${price}")


def test_postgres():
    print("\n=== TEST POSTGRESQL ===")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename;")
    tables = [r[0] for r in cur.fetchall()]
    print(f"  Tablas disponibles: {tables}")

    cur.execute("SELECT COUNT(*) FROM exchanges;")
    n = cur.fetchone()[0]
    print(f"  Exchanges en DB: {n}")

    cur.execute("SELECT name, code FROM exchanges ORDER BY id LIMIT 5;")
    for row in cur.fetchall():
        print(f"    {row[0]} ({row[1]})")

    cur.close()
    conn.close()
    print("  Conexión PostgreSQL OK")


if __name__ == "__main__":
    try:
        test_postgres()
    except Exception as e:
        print(f"  ERROR PostgreSQL: {e}")

    try:
        test_tor()
    except Exception as e:
        print(f"  ERROR Tor: {e}")
