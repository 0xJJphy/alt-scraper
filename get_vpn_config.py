import requests
import base64
import random
import sys

def get_vpn_config():
    url = "http://www.vpngate.net/api/iphone/"
    try:
        print("[VPN] Fetching server list from VPN Gate...")
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        
        lines = resp.text.splitlines()
        if len(lines) < 2:
            print("[ERROR] Failed to fetch valid VPN list.")
            return False
            
        # CSV format: #HostName,IP,Score,Ping,Speed,CountryLong,CountryShort,NumVpnConnections,Operator,Message,OpenVPN_ConfigData_Base64
        # Columns (0-indexed): 5 = CountryLong, 14 = ConfigDataData
        servers = []
        for line in lines[2:]: # Skip header lines
            if not line.strip() or line.startswith("*"):
                continue
            cols = line.split(',')
            if len(cols) < 15:
                continue
                
            country = cols[5]
            if country.lower() in ["united states", "us"]:
                continue
            
            servers.append({
                "ip": cols[1],
                "score": int(cols[2]),
                "country": country,
                "config_b64": cols[14]
            })
            
        if not servers:
            print("[ERROR] No suitable non-US servers found.")
            return False
            
        # Select one of the top 10 servers by score, preferring TCP if available for better GHA compatibility
        servers.sort(key=lambda x: x["score"], reverse=True)
        # Try to find a TCP/443 server in top 10
        candidates = servers[:10]
        
        # We don't have easy info if it's TCP/UDP in this CSV without parsing the config
        # but usually higher score servers are more stable.
        best = random.choice(candidates)
        
        print(f"[VPN] Selected server in {best['country']} (IP: {best['ip']})")
        
        config_data = base64.b64decode(best['config_b64']).decode('utf-8')
        
        # Ensure it has routing directives if missing
        if "redirect-gateway def1" not in config_data:
            config_data += "\nredirect-gateway def1\n"
        
        with open("client.ovpn", "w") as f:
            f.write(config_data)
            # Add directive to prevent it from asking for credentials and ignore some errors
            f.write("\nauth-nocache\n")
            f.write("verb 3\n")
            f.write("mute-replay-warnings\n")
            # For GHA stability
            f.write("connect-retry 2 5\n")
            f.write("resolv-retry infinite\n")
            
        print("[VPN] client.ovpn generated successfully.")
        return True
        
    except Exception as e:
        print(f"[ERROR] VPN Config fetch failed: {e}")
        return False

if __name__ == "__main__":
    if get_vpn_config():
        sys.exit(0)
    else:
        sys.exit(1)
