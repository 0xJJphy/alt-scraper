import requests
import base64
import random
import sys
import re

# Countries known to work with Binance (non-US, good connectivity)
PREFERRED_COUNTRIES = ["japan", "korea", "singapore", "germany", "netherlands", "united kingdom", "canada"]

def is_tcp_config(config_b64: str) -> bool:
    """Check if the OpenVPN config uses TCP protocol (more reliable in GitHub Actions)."""
    try:
        config = base64.b64decode(config_b64).decode('utf-8', errors='ignore')
        # Look for proto tcp or remote with tcp
        if 'proto tcp' in config.lower():
            return True
        # Some configs have "remote host port tcp"
        if re.search(r'remote\s+\S+\s+\d+\s+tcp', config.lower()):
            return True
        return False
    except:
        return False

def get_vpn_config():
    """
    Fetch VPN config from VPN Gate, preferring TCP servers for GitHub Actions compatibility.

    GitHub Actions runners often have UDP traffic limited/blocked, so TCP is more reliable.
    """
    url = "http://www.vpngate.net/api/iphone/"
    try:
        print("[VPN] Fetching server list from VPN Gate...")
        resp = requests.get(url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        resp.raise_for_status()

        lines = resp.text.splitlines()
        if len(lines) < 2:
            print("[ERROR] Failed to fetch valid VPN list.")
            return False

        # CSV format: #HostName,IP,Score,Ping,Speed,CountryLong,CountryShort,...,OpenVPN_ConfigData_Base64
        tcp_servers = []
        udp_servers = []

        for line in lines[2:]:
            if not line.strip() or line.startswith("*"):
                continue
            cols = line.split(',')
            if len(cols) < 15:
                continue

            country = cols[5].lower()
            if country in ["united states", "us"]:
                continue

            config_b64 = cols[14]
            if not config_b64:
                continue

            server = {
                "ip": cols[1],
                "score": int(cols[2]) if cols[2].isdigit() else 0,
                "ping": int(cols[3]) if cols[3].isdigit() else 999,
                "speed": int(cols[4]) if cols[4].isdigit() else 0,
                "country": cols[5],
                "config_b64": config_b64,
                "is_preferred": country in PREFERRED_COUNTRIES
            }

            if is_tcp_config(config_b64):
                tcp_servers.append(server)
            else:
                udp_servers.append(server)

        print(f"[VPN] Found {len(tcp_servers)} TCP servers and {len(udp_servers)} UDP servers")

        # Prefer TCP servers (more reliable in GitHub Actions where UDP may be limited)
        if tcp_servers:
            # Sort by: preferred country first, then by score, then by ping
            tcp_servers.sort(key=lambda x: (not x['is_preferred'], -x['score'], x['ping']))
            candidates = tcp_servers[:20]
            server_type = "TCP"
        elif udp_servers:
            print("[VPN] Warning: No TCP servers found, falling back to UDP")
            udp_servers.sort(key=lambda x: (not x['is_preferred'], -x['score'], x['ping']))
            candidates = udp_servers[:20]
            server_type = "UDP"
        else:
            print("[ERROR] No suitable servers found.")
            return False

        # Pick a random server from top candidates to distribute load
        selected = random.choice(candidates[:10])

        print(f"[VPN] Selected {server_type} server in {selected['country']}")
        print(f"[VPN]   IP: {selected['ip']}, Score: {selected['score']}, Ping: {selected['ping']}ms")

        config_data = base64.b64decode(selected['config_b64']).decode('utf-8')

        # Ensure it has routing directives
        if "redirect-gateway def1" not in config_data:
            config_data += "\nredirect-gateway def1 bypass-dhcp\n"

        # Add GHA compatibility and stability directives
        extra_directives = [
            "auth-nocache",
            "verb 3",
            "mute-replay-warnings",
            "connect-retry 3 10",
            "connect-retry-max 5",
            "resolv-retry 60",
            "nobind",
            "persist-key",
            "persist-tun",
            "pull-filter ignore \"auth-token\"",
            "route-metric 1",
            "sndbuf 524288",
            "rcvbuf 524288",
        ]

        with open("client.ovpn", "w") as f:
            f.write(config_data)
            f.write("\n# GitHub Actions optimizations\n")
            for d in extra_directives:
                if d.split()[0] not in config_data:
                    f.write(f"{d}\n")

        print("[VPN] client.ovpn generated successfully.")
        return True

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Network error fetching VPN list: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] VPN Config fetch failed: {e}")
        return False

if __name__ == "__main__":
    if get_vpn_config():
        sys.exit(0)
    else:
        sys.exit(1)
