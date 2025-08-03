# v2ray_collector.py
# ===== IMPORTS & DEPENDENCIES =====
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import logging
import re
import base64
import json
import subprocess
import socket
from pathlib import Path
from typing import List, Set, Dict, Optional, TypedDict
from bs4 import BeautifulSoup
# ===== CONFIGURATION & CONSTANTS =====
# Directories and Files
OUTPUT_DIR = Path("v2ray_configs")
VALIDATED_DIR = Path("validated_configs")
CHANNELS_FILE = Path("channels.txt")
RESULTS_JSON_FILE = VALIDATED_DIR / "results.json"
# Concurrency Settings
# Reduce validator workers to avoid overwhelming the free geo-ip API
SCRAPER_WORKERS = 10
VALIDATOR_WORKERS = 15 # Reduced from 50 to be less aggressive
# Xray Configuration
# For local testing, point this to the xray executable.
# In GitHub Actions, it will be available in the path.
XRAY_PATH = "xray" 
XRAY_CONFIG_FILE = Path("xray_config.json")

# Network and API Configuration
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
REQUEST_TIMEOUT = 10
GEO_IP_API_URL = "http://ip-api.com/json/{ip}?fields=status,country,countryCode,isp"

# A small utility for country code to flag emoji
COUNTRY_FLAGS = {
    "US": "🇺🇸", "DE": "🇩🇪", "FR": "🇫🇷", "GB": "🇬🇧", "CA": "🇨🇦", "NL": "🇳🇱",
    "SG": "🇸🇬", "JP": "🇯🇵", "HK": "🇭🇰", "AU": "🇦🇺", "CH": "🇨🇭", "SE": "🇸🇪",
    "FI": "🇫🇮", "NO": "🇳🇴", "IE": "🇮🇪", "IR": "🇮🇷", "TR": "🇹🇷", "RU": "🇷🇺",
    # Add more as needed
}

# ===== TYPE DEFINITIONS =====
class ValidatedConfig(TypedDict):
    config: str
    protocol: str
    name: str
    country_code: str
    country_name: str
    isp: str
    channel: str

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO, # Changed to INFO
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ===== CORE LOGIC & UTILITY FUNCTIONS =====

def create_requests_session(pool_size: int = 100) -> requests.Session:
    """
    Creates a requests Session with a custom-sized connection pool and a robust retry strategy.
    
    Args:
        pool_size: The maximum number of connections to keep in the pool.
    
    Returns:
        A configured requests.Session object.
    """
    session = requests.Session()
    
    # Define a comprehensive retry strategy for network instability
    retry_strategy = Retry(
        total=5,                # Total number of retries
        read=5,                 # Number of retries on read errors
        connect=5,              # Number of retries on connection errors
        backoff_factor=1,       # A delay factor between retries: {backoff factor} * (2 ** ({number of total retries} - 1))
        status_forcelist=[429, 502, 503, 504], # Retry on these server errors (500 is often a permanent server bug)
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(
        pool_connections=pool_size, 
        pool_maxsize=pool_size, 
        max_retries=retry_strategy
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def load_channels(file_path: Path) -> List[str]:
    """Loads Telegram channel names from a text file."""
    if not file_path.exists():
        logging.error(f"Channels file not found at: {file_path}")
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        channels = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
    logging.info(f"Loaded {len(channels)} channels from {file_path}")
    return channels

def fetch_channel_content(session: requests.Session, channel_name: str) -> Optional[str]:
    """Fetches the HTML content of a public Telegram channel."""
    url = f"https://t.me/s/{channel_name}"
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch content for channel '{channel_name}': {e}")
        return None

def parse_v2ray_configs(html_content: str) -> Set[str]:
    """Parses HTML content to find and extract V2Ray configuration links."""
    configs: Set[str] = set()
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Combined regex to find all supported protocol links
    # This will search the entire text content for any line starting with these protocols
    protocol_pattern = re.compile(r"^(vmess|vless|ss|trojan|hysteria|hy2)://[^\s]+")
    
    # Regex to find potential base64 encoded strings (common in Telegram)
    # This pattern looks for blocks of characters that are typical of base64,
    # often found after a protocol prefix or as standalone blocks.
    base64_pattern = re.compile(r'(vmess://[a-zA-Z0-9+/=]{10,}[^<\s]+)') # Made more robust to include characters until whitespace or HTML tag

    # Extract text from the entire HTML content
    full_text_content = soup.get_text(separator="\n").strip()

    # 1. Extract direct protocol links from all text
    for line in full_text_content.splitlines():
        clean_line = line.strip()
        if protocol_pattern.match(clean_line):
            configs.add(clean_line)

    # 2. Extract base64 encoded vmess links from all text
    for match in base64_pattern.finditer(full_text_content):
        encoded_str = match.group(0) # Get the full matched string (e.g., "vmess://...")
        try:
            # Attempt to decode and re-add if it's a valid vmess link
            decoded_bytes = base64.b64decode(encoded_str[8:]) # Skip 'vmess://'
            decoded_json = json.loads(decoded_bytes.decode('utf-8'))
            # Reconstruct the vmess link to ensure consistency and validity
            configs.add(f"vmess://{base64.b64encode(json.dumps(decoded_json).encode('utf-8')).decode('utf-8')}")
        except Exception as e:
            # Log error but don't fail the entire process for one bad config
            logging.debug(f"Failed to decode or parse base64 config: {encoded_str}. Error: {e}")
    
    return configs

def scrape_channel(session: requests.Session, channel_name: str) -> List[tuple[str, str]]:
    """Scrapes a single Telegram channel for configs and returns them with the channel name."""
    logging.info(f"Scraping channel: {channel_name}")
    html_content = fetch_channel_content(session, channel_name)
    if not html_content:
        return []
    configs = parse_v2ray_configs(html_content)
    return [(config, channel_name) for config in configs]

def get_server_ip(config_str: str) -> Optional[str]:
    """Extracts server address from config and resolves to IP."""
    address = None
    if config_str.startswith("vmess://"):
        try:
            # Try decoding with utf-8 first, then fall back to latin-1 for broader compatibility
            decoded_part = base64.b64decode(config_str[8:]).decode('utf-8')
            data = json.loads(decoded_part)
            address = data.get("add")
        except UnicodeDecodeError:
            try:
                decoded_part = base64.b64decode(config_str[8:]).decode('latin-1')
                data = json.loads(decoded_part)
                address = data.get("add")
            except Exception as e:
                logging.debug(f"Failed to decode vmess config with latin-1: {config_str[:50]}... Error: {e}")
                return None
        except Exception as e:
            logging.debug(f"Failed to decode vmess config: {config_str[:50]}... Error: {e}")
            return None
    else:
        # Improved regex for vless, trojan, ss to capture hostname more accurately
        # It looks for a sequence of non-@, non-?, non-# characters after :// and optional user@
        match = re.search(r"://(?:[^@/?#]*@)?([^/:?#]+)", config_str)
        if match:
            address = match.group(1)
        else:
            logging.debug(f"Could not parse address from config: {config_str[:50]}...")
            return None

    if not address:
        logging.debug(f"Address not found in config: {config_str[:50]}...")
        return None
    
    # Filter out private IP ranges and localhosts
    private_ip_patterns = [
        re.compile(r"^10\.\d{1,3}\.\d{1,3}\.\d{1,3}$"),
        re.compile(r"^172\.(1[6-9]|2[0-9]|3[0-1])\.\d{1,3}\.\d{1,3}$"),
        re.compile(r"^192\.168\.\d{1,3}\.\d{1,3}$"),
        re.compile(r"^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$"), # Localhost
        re.compile(r"^0\.0\.0\.0$") # Invalid IP
    ]
    if re.match(r"\d{1,3}(\.\d{1,3}){3}", address): # Already an IP
        if any(pattern.match(address) for pattern in private_ip_patterns):
            logging.debug(f"Skipping private/invalid IP address: {address} from config {config_str[:50]}...")
            return None
        return address
    
    try:
        return socket.gethostbyname(address)
    except socket.gaierror as e:
        logging.debug(f"DNS resolution failed for {address} from config {config_str[:50]}... Error: {e}")
        return None
    except Exception as e:
        logging.debug(f"An unexpected error occurred while getting server IP for {address} from config {config_str[:50]}... Error: {e}")
        return None

def get_geo_info(session: requests.Session, ip_address: str) -> Dict:
    """Gets geographic information for an IP address."""
    try:
        response = session.get(GEO_IP_API_URL.format(ip=ip_address), timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "success":
            return {
                "country_code": data.get("countryCode", "N/A"),
                "country_name": data.get("country", "Unknown"),
                "isp": data.get("isp", "Unknown ISP"),
            }
    except requests.exceptions.RequestException:
        pass # Silently fail
    return {"country_code": "N/A", "country_name": "Unknown", "isp": "Unknown ISP"}

def get_batch_geo_info(session: requests.Session, ip_list: List[str]) -> Dict[str, Dict]:
    """Gets geographic information for a list of IP addresses in a batch."""
    # The ip-api.com allows batch queries up to 100 IPs
    # https://ip-api.com/docs/api:batch
    batch_url = "http://ip-api.com/batch"
    results = {}
    
    # Break the list into chunks of 100
    for i in range(0, len(ip_list), 100):
        chunk = ip_list[i:i+100]
        try:
            response = session.post(batch_url, json=chunk, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            batch_data = response.json()
            for item in batch_data:
                ip = item.get("query")
                if ip and item.get("status") == "success":
                    results[ip] = {
                        "country_code": item.get("countryCode", "N/A"),
                        "country_name": item.get("country", "Unknown"),
                        "isp": item.get("isp", "Unknown ISP"),
                    }
                elif ip:
                    results[ip] = {"country_code": "N/A", "country_name": "Unknown", "isp": "Unknown ISP"}
        except requests.exceptions.RequestException as e:
            logging.debug(f"Batch geo-IP lookup failed: {e}")
            # If batch fails, try individual lookups for this chunk
            for ip in chunk:
                results[ip] = get_geo_info(session, ip) # Fallback to individual lookup
    return results


def validate_and_enrich_config(config_with_channel: tuple[str, str], geo_ip_cache: Dict[str, Dict]) -> Optional[ValidatedConfig]:
    """Validates a single config, enriches it with geo data, and returns structured data."""
    config, channel = config_with_channel
    
    ip = get_server_ip(config)
    if not ip:
        return None

    geo_info = geo_ip_cache.get(ip, {"country_code": "N/A", "country_name": "Unknown", "isp": "Unknown ISP"})
    protocol_match = re.match(r"(\w+):?//", config)
    protocol = protocol_match.group(1).lower() if protocol_match else "unknown"

    # --- FIX: Create a URL-safe and client-friendly name ---
    # We remove emojis as they cause encoding issues in many V2Ray clients.
    country_code = geo_info.get("country_code", "N/A").upper()
    isp = geo_info.get("isp", "Unknown").split()[0] # Use only the first word of the ISP for brevity
    
    # Example name: DE-Hetzner-VLESS
    name = f"{country_code}-{isp}-{protocol.upper()}"
    
    # Also create a display name with flag for the dashboard
    flag = COUNTRY_FLAGS.get(country_code, "❓")
    display_name = f"[{country_code}]{flag} {isp}"
    # --- END FIX ---
    # Create the renamed config string for subscription files
    renamed_config = config.split("#")[0] + f"#{name}"
    # --- FIX END ---
    
    return {
        "config": config,
        "renamed_config": renamed_config,
        "protocol": protocol,
        "name": name, # The URL-safe name
        "display_name": display_name, # The pretty name for the UI
        "country_code": country_code,
        "country_name": geo_info["country_name"],
        "isp": geo_info["isp"],
        "channel": channel
    }

def save_results(results: List[ValidatedConfig]):
    """
    Saves the validated configs to JSON and creates subscription files
    grouped by protocol and by country.
    """
    if not results:
        logging.info("No valid configs to save.")
        # Create an empty results file to prevent 404 on the dashboard
        VALIDATED_DIR.mkdir(exist_ok=True)
        with open(RESULTS_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)
        return

    VALIDATED_DIR.mkdir(exist_ok=True)
    
    # --- 1. Save detailed JSON results for the dashboard ---
    with open(RESULTS_JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    logging.info(f"Saved {len(results)} validated configs to {RESULTS_JSON_FILE}")

    # --- 2. Group by Protocol and save subscription files ---
    grouped_by_protocol: Dict[str, List[str]] = {}
    all_renamed_configs = []
    
    for res in results:
        protocol = res["protocol"]
        if protocol not in grouped_by_protocol:
            grouped_by_protocol[protocol] = []
        
        # Use the pre-renamed config for subscriptions
        renamed_config = res.get("renamed_config", res["config"]) # Fallback for safety
        grouped_by_protocol[protocol].append(renamed_config)
        all_renamed_configs.append(renamed_config)
    
    # Save individual protocol files
    for protocol, configs in grouped_by_protocol.items():
        file_path = VALIDATED_DIR / f"{protocol}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(configs))
        logging.info(f"Saved {len(configs)} {protocol} configs to {file_path}")
        
    # Save combined subscription file (all protocols)
    combined_path = VALIDATED_DIR / "subscription.txt"
    encoded_all = base64.b64encode("\n".join(all_renamed_configs).encode("utf-8")).decode("utf-8")
    with open(combined_path, "w", encoding="utf-8") as f:
        f.write(encoded_all)
    logging.info(f"Saved combined subscription file to {combined_path}")

    # --- 3. Group by Country and save subscription files ---
    country_dir = VALIDATED_DIR / "by-country"
    country_dir.mkdir(exist_ok=True)
    
    grouped_by_country: Dict[str, List[str]] = {}
    for res in results:
        country_code = res.get("country_code", "N/A").upper()
        if country_code not in grouped_by_country:
            grouped_by_country[country_code] = []
        renamed_config = res.get("renamed_config", res["config"])
        grouped_by_country[country_code].append(renamed_config)

    # Save individual country subscription files
    for country_code, configs in grouped_by_country.items():
        if country_code == "N/A":
            continue # Skip saving a file for unknown countries
            
        country_file_path = country_dir / f"{country_code}.txt"
        encoded_country_configs = base64.b64encode("\n".join(configs).encode("utf-8")).decode("utf-8")
        with open(country_file_path, "w", encoding="utf-8") as f:
            f.write(encoded_country_configs)
        logging.info(f"Saved {len(configs)} configs for country {country_code} to {country_file_path}")

    # --- 4. Group by Channel and save subscription files ---
    channel_sub_dir = VALIDATED_DIR / "by-channel"
    channel_sub_dir.mkdir(exist_ok=True)
    
    grouped_by_channel: Dict[str, List[str]] = {}
    for res in results:
        channel = res.get("channel", "Unknown")
        if channel not in grouped_by_channel:
            grouped_by_channel[channel] = []
        renamed_config = res.get("renamed_config", res["config"])
        grouped_by_channel[channel].append(renamed_config)

    # Save individual channel subscription files
    for channel, configs in grouped_by_channel.items():
        if channel == "Unknown" or not configs:
            continue

        channel_file_path = channel_sub_dir / f"{channel}.txt"
        encoded_channel_configs = base64.b64encode("\n".join(configs).encode("utf-8")).decode("utf-8")
        with open(channel_file_path, "w", encoding="utf-8") as f:
            f.write(encoded_channel_configs)
        logging.info(f"Saved {len(configs)} configs for channel {channel} to {channel_file_path}")


# ===== INITIALIZATION & STARTUP =====
def main():
    """Main function to orchestrate the scraping and validation process."""
    channels = load_channels(CHANNELS_FILE)
    if not channels:
        logging.warning("No channels to scrape. Exiting.")
        return

    session = create_requests_session()
    session.headers.update({"User-Agent": USER_AGENT})

    # Phase 1: Scraping
    logging.info("--- Starting Scraping Phase ---")
    raw_configs_with_channel: List[tuple[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=SCRAPER_WORKERS, thread_name_prefix='Scraper') as executor:
        future_to_channel = {executor.submit(scrape_channel, session, name): name for name in channels}
        for future in concurrent.futures.as_completed(future_to_channel):
            try:
                channel_configs = future.result()
                if channel_configs:
                    raw_configs_with_channel.extend(channel_configs)
            except Exception as e:
                channel_name = future_to_channel[future]
                logging.error(f"An exception occurred while processing channel {channel_name}: {e}")

    # Remove duplicate configs, keeping the first channel it was seen in
    raw_configs = list(dict.fromkeys(raw_configs_with_channel))

    logging.info(f"--- Scraping Complete --- Found {len(raw_configs)} unique potential configs.")

    if not raw_configs:
        logging.info("No configs found to validate. Exiting.")
        save_results([]) 
        return

    # Phase 2: Validation and Enrichment
    logging.info("--- Starting Validation and Enrichment Phase ---")
    
    # Collect all unique IPs for batch Geo-IP lookup
    unique_ips = set()
    configs_with_ips = [] # Store (config, channel, ip) tuples
    for cfg_with_channel in raw_configs:
        config_str, channel = cfg_with_channel
        ip = get_server_ip(config_str)
        if ip:
            unique_ips.add(ip)
            configs_with_ips.append((config_str, channel, ip))

    # Perform batch Geo-IP lookup
    logging.info(f"Performing batch Geo-IP lookup for {len(unique_ips)} unique IPs...")
    geo_ip_cache = get_batch_geo_info(session, list(unique_ips))
    logging.info("Batch Geo-IP lookup complete.")

    validated_configs: List[ValidatedConfig] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=VALIDATOR_WORKERS, thread_name_prefix='Validator') as executor:
        # Pass the geo_ip_cache to the validation function
        future_to_config = {executor.submit(validate_and_enrich_config, (cfg, ch), geo_ip_cache): (cfg, ch) for cfg, ch, _ in configs_with_ips}
        
        processed_count = 0
        total_count = len(configs_with_ips) # Use count of configs with valid IPs
        for future in concurrent.futures.as_completed(future_to_config):
            processed_count += 1
            try:
                result = future.result()
                if result:
                    validated_configs.append(result)
            except Exception as e:
                config_tuple = future_to_config[future]
                logging.error(f"An exception occurred while validating config {config_tuple[0][:30]}...: {e}")
            
            if processed_count % 20 == 0 or processed_count == total_count:
                logging.info(f"Validation progress: {processed_count}/{total_count} configs processed.")

    
    
    logging.info(f"--- Validation Complete --- Found {len(validated_configs)} working configs.")

    # Phase 3: Save results
    save_results(validated_configs)

    session.close()


if __name__ == "__main__":

    main()
