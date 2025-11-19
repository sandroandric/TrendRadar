# coding=utf-8

import json
import os
import random
import re
import time
import webbrowser
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr, formatdate, make_msgid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union

import pytz
import requests
import yaml


VERSION = "3.0.5"


# === SMTP Email Configuration ===
# Common Email Provider Configurations
SMTP_CONFIGS = {
    # Gmail (Use STARTTLS)
    "gmail.com": {"server": "smtp.gmail.com", "port": 587, "encryption": "TLS"},
    # QQ Mail (Use SSL, more stable)
    "qq.com": {"server": "smtp.qq.com", "port": 465, "encryption": "SSL"},
    # Outlook (Use STARTTLS)
    "outlook.com": {
        "server": "smtp-mail.outlook.com",
        "port": 587,
        "encryption": "TLS",
    },
    "hotmail.com": {
        "server": "smtp-mail.outlook.com",
        "port": 587,
        "encryption": "TLS",
    },
    "live.com": {"server": "smtp-mail.outlook.com", "port": 587, "encryption": "TLS"},
    # NetEase Mail (Use SSL, more stable)
    "163.com": {"server": "smtp.163.com", "port": 465, "encryption": "SSL"},
    "126.com": {"server": "smtp.126.com", "port": 465, "encryption": "SSL"},
    # Sina Mail (Use SSL)
    "sina.com": {"server": "smtp.sina.com", "port": 465, "encryption": "SSL"},
    # Sohu Mail (Use SSL)
    "sohu.com": {"server": "smtp.sohu.com", "port": 465, "encryption": "SSL"},
}


# === Configuration Management ===
def load_config():
    """Load configuration file"""
    config_path = os.environ.get("CONFIG_PATH", "config/config.yaml")

    if not Path(config_path).exists():
        raise FileNotFoundError(f"Config file {config_path} not found")

    with open(config_path, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f)

    print(f"Configuration loaded successfully: {config_path}")

    # Build configuration
    config = {
        "VERSION_CHECK_URL": config_data["app"]["version_check_url"],
        "SHOW_VERSION_UPDATE": config_data["app"]["show_version_update"],
        "REQUEST_INTERVAL": config_data["crawler"]["request_interval"],
        "REPORT_MODE": os.environ.get("REPORT_MODE", "").strip()
        or config_data["report"]["mode"],
        "RANK_THRESHOLD": config_data["report"]["rank_threshold"],
        "USE_PROXY": config_data["crawler"]["use_proxy"],
        "DEFAULT_PROXY": config_data["crawler"]["default_proxy"],
        "ENABLE_CRAWLER": os.environ.get("ENABLE_CRAWLER", "").strip().lower()
        in ("true", "1")
        if os.environ.get("ENABLE_CRAWLER", "").strip()
        else config_data["crawler"]["enable_crawler"],
        "ENABLE_NOTIFICATION": os.environ.get("ENABLE_NOTIFICATION", "").strip().lower()
        in ("true", "1")
        if os.environ.get("ENABLE_NOTIFICATION", "").strip()
        else config_data["notification"]["enable_notification"],
        "MESSAGE_BATCH_SIZE": config_data["notification"]["message_batch_size"],
        "DINGTALK_BATCH_SIZE": config_data["notification"].get(
            "dingtalk_batch_size", 20000
        ),
        "FEISHU_BATCH_SIZE": config_data["notification"].get("feishu_batch_size", 29000),
        "BATCH_SEND_INTERVAL": config_data["notification"]["batch_send_interval"],
        "FEISHU_MESSAGE_SEPARATOR": config_data["notification"][
            "feishu_message_separator"
        ],
        "PUSH_WINDOW": {
            "ENABLED": os.environ.get("PUSH_WINDOW_ENABLED", "").strip().lower()
            in ("true", "1")
            if os.environ.get("PUSH_WINDOW_ENABLED", "").strip()
            else config_data["notification"]
            .get("push_window", {})
            .get("enabled", False),
            "TIME_RANGE": {
                "START": os.environ.get("PUSH_WINDOW_START", "").strip()
                or config_data["notification"]
                .get("push_window", {})
                .get("time_range", {})
                .get("start", "08:00"),
                "END": os.environ.get("PUSH_WINDOW_END", "").strip()
                or config_data["notification"]
                .get("push_window", {})
                .get("time_range", {})
                .get("end", "22:00"),
            },
            "ONCE_PER_DAY": os.environ.get("PUSH_WINDOW_ONCE_PER_DAY", "").strip().lower()
            in ("true", "1")
            if os.environ.get("PUSH_WINDOW_ONCE_PER_DAY", "").strip()
            else config_data["notification"]
            .get("push_window", {})
            .get("once_per_day", True),
            "RECORD_RETENTION_DAYS": int(
                os.environ.get("PUSH_WINDOW_RETENTION_DAYS", "").strip() or "0"
            )
            or config_data["notification"]
            .get("push_window", {})
            .get("push_record_retention_days", 7),
        },
        "WEIGHT_CONFIG": {
            "RANK_WEIGHT": config_data["weight"]["rank_weight"],
            "FREQUENCY_WEIGHT": config_data["weight"]["frequency_weight"],
            "HOTNESS_WEIGHT": config_data["weight"]["hotness_weight"],
        },
        "PLATFORMS": config_data["platforms"],
    }

    # Notification channel configuration (Environment variables take precedence)
    notification = config_data.get("notification", {})
    webhooks = notification.get("webhooks", {})

    config["FEISHU_WEBHOOK_URL"] = os.environ.get(
        "FEISHU_WEBHOOK_URL", ""
    ).strip() or webhooks.get("feishu_url", "")
    config["DINGTALK_WEBHOOK_URL"] = os.environ.get(
        "DINGTALK_WEBHOOK_URL", ""
    ).strip() or webhooks.get("dingtalk_url", "")
    config["WEWORK_WEBHOOK_URL"] = os.environ.get(
        "WEWORK_WEBHOOK_URL", ""
    ).strip() or webhooks.get("wework_url", "")
    config["TELEGRAM_BOT_TOKEN"] = os.environ.get(
        "TELEGRAM_BOT_TOKEN", ""
    ).strip() or webhooks.get("telegram_bot_token", "")
    config["TELEGRAM_CHAT_ID"] = os.environ.get(
        "TELEGRAM_CHAT_ID", ""
    ).strip() or webhooks.get("telegram_chat_id", "")

    # Email configuration
    config["EMAIL_FROM"] = os.environ.get("EMAIL_FROM", "").strip() or webhooks.get(
        "email_from", ""
    )
    config["EMAIL_PASSWORD"] = os.environ.get(
        "EMAIL_PASSWORD", ""
    ).strip() or webhooks.get("email_password", "")
    config["EMAIL_TO"] = os.environ.get("EMAIL_TO", "").strip() or webhooks.get(
        "email_to", ""
    )
    config["EMAIL_SMTP_SERVER"] = os.environ.get(
        "EMAIL_SMTP_SERVER", ""
    ).strip() or webhooks.get("email_smtp_server", "")
    config["EMAIL_SMTP_PORT"] = os.environ.get(
        "EMAIL_SMTP_PORT", ""
    ).strip() or webhooks.get("email_smtp_port", "")

    # ntfy configuration
    config["NTFY_SERVER_URL"] = os.environ.get(
        "NTFY_SERVER_URL", "https://ntfy.sh"
    ).strip() or webhooks.get("ntfy_server_url", "https://ntfy.sh")
    config["NTFY_TOPIC"] = os.environ.get("NTFY_TOPIC", "").strip() or webhooks.get(
        "ntfy_topic", ""
    )
    config["NTFY_TOKEN"] = os.environ.get("NTFY_TOKEN", "").strip() or webhooks.get(
        "ntfy_token", ""
    )

    # Output configuration source information
    notification_sources = []
    if config["FEISHU_WEBHOOK_URL"]:
        source = "Env Var" if os.environ.get("FEISHU_WEBHOOK_URL") else "Config File"
        notification_sources.append(f"Feishu({source})")
    if config["DINGTALK_WEBHOOK_URL"]:
        source = "Env Var" if os.environ.get("DINGTALK_WEBHOOK_URL") else "Config File"
        notification_sources.append(f"DingTalk({source})")
    if config["WEWORK_WEBHOOK_URL"]:
        source = "Env Var" if os.environ.get("WEWORK_WEBHOOK_URL") else "Config File"
        notification_sources.append(f"WeWork({source})")
    if config["TELEGRAM_BOT_TOKEN"] and config["TELEGRAM_CHAT_ID"]:
        token_source = (
            "Env Var" if os.environ.get("TELEGRAM_BOT_TOKEN") else "Config File"
        )
        chat_source = "Env Var" if os.environ.get("TELEGRAM_CHAT_ID") else "Config File"
        notification_sources.append(f"Telegram({token_source}/{chat_source})")
    if config["EMAIL_FROM"] and config["EMAIL_PASSWORD"] and config["EMAIL_TO"]:
        from_source = "Env Var" if os.environ.get("EMAIL_FROM") else "Config File"
        notification_sources.append(f"Email({from_source})")

    if config["NTFY_SERVER_URL"] and config["NTFY_TOPIC"]:
        server_source = "Env Var" if os.environ.get("NTFY_SERVER_URL") else "Config File"
        notification_sources.append(f"ntfy({server_source})")

    if notification_sources:
        print(f"Notification channels configured from: {', '.join(notification_sources)}")
    else:
        print("No notification channels configured")

    return config


print("Loading configuration...")
CONFIG = load_config()
print(f"TrendRadar v{VERSION} configuration loaded")
print(f"Monitored platforms: {len(CONFIG['PLATFORMS'])}")


# === Utility Functions ===
def get_beijing_time():
    """Get Beijing Time"""
    return datetime.now(pytz.timezone("Asia/Shanghai"))


def format_date_folder():
    """Format date folder name"""
    return get_beijing_time().strftime("%Y-%m-%d")


def format_time_filename():
    """Format time filename"""
    return get_beijing_time().strftime("%H-%M")


def clean_title(title: str) -> str:
    """Clean special characters in title"""
    if not isinstance(title, str):
        title = str(title)
    cleaned_title = title.replace("\n", " ").replace("\r", " ")
    cleaned_title = re.sub(r"\s+", " ", cleaned_title)
    cleaned_title = cleaned_title.strip()
    return cleaned_title


def ensure_directory_exists(directory: str):
    """Ensure directory exists"""
    Path(directory).mkdir(parents=True, exist_ok=True)


def get_output_path(type_dir: str, filename: str) -> str:
    """Get output path"""
    date_folder = format_date_folder()
    output_dir = Path("output") / date_folder / type_dir
    ensure_directory_exists(str(output_dir))
    return str(output_dir / filename)


def check_version_update(
    current_version: str, version_url: str, proxy_url: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """Check for version updates"""
    try:
        proxies = None
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/plain, */*",
            "Cache-Control": "no-cache",
        }

        response = requests.get(
            version_url, proxies=proxies, headers=headers, timeout=10
        )
        response.raise_for_status()

        remote_version = response.text.strip()
        print(f"Current version: {current_version}, Remote version: {remote_version}")

        # Compare versions
        def parse_version(version_str):
            try:
                parts = version_str.strip().split(".")
                if len(parts) != 3:
                    raise ValueError("Invalid version format")
                return int(parts[0]), int(parts[1]), int(parts[2])
            except:
                return 0, 0, 0

        current_tuple = parse_version(current_version)
        remote_tuple = parse_version(remote_version)

        need_update = current_tuple < remote_tuple
        return need_update, remote_version if need_update else None

    except Exception as e:
        print(f"Version check failed: {e}")
        return False, None


def is_first_crawl_today() -> bool:
    """Check if it is the first crawl of the day"""
    date_folder = format_date_folder()
    txt_dir = Path("output") / date_folder / "txt"

    if not txt_dir.exists():
        return True

    files = sorted([f for f in txt_dir.iterdir() if f.suffix == ".txt"])
    return len(files) <= 1


def html_escape(text: str) -> str:
    """HTML Escape"""
    if not isinstance(text, str):
        text = str(text)

    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


# === Push Record Management ===
class PushRecordManager:
    """Push Record Manager"""

    def __init__(self):
        self.record_dir = Path("output") / ".push_records"
        self.ensure_record_dir()
        self.cleanup_old_records()

    def ensure_record_dir(self):
        """Ensure record directory exists"""
        self.record_dir.mkdir(parents=True, exist_ok=True)

    def get_today_record_file(self) -> Path:
        """Get today's record file path"""
        today = get_beijing_time().strftime("%Y%m%d")
        return self.record_dir / f"push_record_{today}.json"

    def cleanup_old_records(self):
        """Clean up old push records"""
        retention_days = CONFIG["PUSH_WINDOW"]["RECORD_RETENTION_DAYS"]
        current_time = get_beijing_time()

        for record_file in self.record_dir.glob("push_record_*.json"):
            try:
                date_str = record_file.stem.replace("push_record_", "")
                file_date = datetime.strptime(date_str, "%Y%m%d")
                file_date = pytz.timezone("Asia/Shanghai").localize(file_date)

                if (current_time - file_date).days > retention_days:
                    record_file.unlink()
                    print(f"Cleaned up old push record: {record_file.name}")
            except Exception as e:
                print(f"Failed to clean up record file {record_file}: {e}")

    def has_pushed_today(self) -> bool:
        """Check if pushed today"""
        record_file = self.get_today_record_file()

        if not record_file.exists():
            return False

        try:
            with open(record_file, "r", encoding="utf-8") as f:
                record = json.load(f)
            return record.get("pushed", False)
        except Exception as e:
            print(f"Failed to read push record: {e}")
            return False

    def record_push(self, report_type: str):
        """Record push"""
        record_file = self.get_today_record_file()
        now = get_beijing_time()

        record = {
            "pushed": True,
            "push_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "report_type": report_type,
        }

        try:
            with open(record_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            print(f"Push record saved: {report_type} at {now.strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"Failed to save push record: {e}")

    def is_in_time_range(self, start_time: str, end_time: str) -> bool:
        """Check if current time is within the specified range"""
        now = get_beijing_time()
        current_time = now.strftime("%H:%M")
    
        def normalize_time(time_str: str) -> str:
            """Normalize time string to HH:MM format"""
            try:
                parts = time_str.strip().split(":")
                if len(parts) != 2:
                    raise ValueError(f"Invalid time format: {time_str}")
            
                hour = int(parts[0])
                minute = int(parts[1])
            
                if not (0 <= hour <= 23 and 0 <= minute <= 59):
                    raise ValueError(f"Time range error: {time_str}")
            
                return f"{hour:02d}:{minute:02d}"
            except Exception as e:
                print(f"Time formatting error '{time_str}': {e}")
                return time_str
    
        normalized_start = normalize_time(start_time)
        normalized_end = normalize_time(end_time)
        normalized_current = normalize_time(current_time)
    
        result = normalized_start <= normalized_current <= normalized_end
    
        if not result:
            print(f"Time window check: Current {normalized_current}, Window {normalized_start}-{normalized_end}")
    
        return result


# === Data Fetching ===
class DataFetcher:
    """Data Fetcher"""

    def __init__(self, proxy_url: Optional[str] = None):
        self.proxy_url = proxy_url

    def fetch_data(
        self,
        id_info: Union[str, Tuple[str, str]],
        max_retries: int = 2,
        min_retry_wait: int = 3,
        max_retry_wait: int = 5,
    ) -> Tuple[Optional[str], str, str]:
        """Fetch data for specified ID, support retry"""
        if isinstance(id_info, tuple):
            id_value, alias = id_info
        else:
            id_value = id_info
            alias = id_value

        url = f"https://newsnow.busiyi.world/api/s?id={id_value}&latest"

        proxies = None
        if self.proxy_url:
            proxies = {"http": self.proxy_url, "https": self.proxy_url}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        }

        retries = 0
        while retries <= max_retries:
            try:
                response = requests.get(
                    url, proxies=proxies, headers=headers, timeout=10
                )
                response.raise_for_status()

                data_text = response.text
                data_json = json.loads(data_text)

                status = data_json.get("status", "Unknown")
                if status not in ["success", "cache"]:
                    raise ValueError(f"Abnormal response status: {status}")

                status_info = "Latest Data" if status == "success" else "Cached Data"
                print(f"Fetch {id_value} success ({status_info})")
                return data_text, id_value, alias

            except Exception as e:
                retries += 1
                if retries <= max_retries:
                    base_wait = random.uniform(min_retry_wait, max_retry_wait)
                    additional_wait = (retries - 1) * random.uniform(1, 2)
                    wait_time = base_wait + additional_wait
                    print(f"Request {id_value} failed: {e}. Retrying in {wait_time:.2f}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Request {id_value} failed: {e}")
                    return None, id_value, alias
        return None, id_value, alias

    def crawl_websites(
        self,
        ids_list: List[Union[str, Tuple[str, str]]],
        request_interval: int = CONFIG["REQUEST_INTERVAL"],
    ) -> Tuple[Dict, Dict, List]:
        """Crawl data from multiple websites"""
        results = {}
        id_to_name = {}
        failed_ids = []

        for i, id_info in enumerate(ids_list):
            if isinstance(id_info, tuple):
                id_value, name = id_info
            else:
                id_value = id_info
                name = id_value

            id_to_name[id_value] = name
            response, _, _ = self.fetch_data(id_info)

            if response:
                try:
                    data = json.loads(response)
                    results[id_value] = {}
                    for index, item in enumerate(data.get("items", []), 1):
                        title = item["title"]
                        url = item.get("url", "")
                        mobile_url = item.get("mobileUrl", "")
                        extra = item.get("extra", {})
                        summary = extra.get("hover", "")

                        if title in results[id_value]:
                            results[id_value][title]["ranks"].append(index)
                            # Update summary if existing one is empty
                            if summary and not results[id_value][title].get("summary"):
                                results[id_value][title]["summary"] = summary
                        else:
                            results[id_value][title] = {
                                "ranks": [index],
                                "url": url,
                                "mobileUrl": mobile_url,
                                "summary": summary,
                            }
                except json.JSONDecodeError:
                    print(f"Failed to parse {id_value} response")
                    failed_ids.append(id_value)
                except Exception as e:
                    print(f"Error processing {id_value} data: {e}")
                    failed_ids.append(id_value)
            else:
                failed_ids.append(id_value)

            if i < len(ids_list) - 1:
                actual_interval = request_interval + random.randint(-10, 20)
                actual_interval = max(50, actual_interval)
                time.sleep(actual_interval / 1000)

        print(f"Success: {list(results.keys())}, Failed: {failed_ids}")
        return results, id_to_name, failed_ids


# === Data Processing ===
def save_titles_to_file(results: Dict, id_to_name: Dict, failed_ids: List) -> str:
    """Save titles to file"""
    file_path = get_output_path("txt", f"{format_time_filename()}.txt")

    with open(file_path, "w", encoding="utf-8") as f:
        for id_value, title_data in results.items():
            # id | name or id
            name = id_to_name.get(id_value)
            if name and name != id_value:
                f.write(f"{id_value} | {name}\n")
            else:
                f.write(f"{id_value}\n")

            # Sort titles by rank
            sorted_titles = []
            for title, info in title_data.items():
                cleaned_title = clean_title(title)
                if isinstance(info, dict):
                    ranks = info.get("ranks", [])
                    url = info.get("url", "")
                    mobile_url = info.get("mobileUrl", "")
                    summary = info.get("summary", "")
                else:
                    ranks = info if isinstance(info, list) else []
                    url = ""
                    mobile_url = ""
                    summary = ""

                rank = ranks[0] if ranks else 1
                sorted_titles.append((rank, cleaned_title, url, mobile_url, summary))

            sorted_titles.sort(key=lambda x: x[0])

            for rank, cleaned_title, url, mobile_url, summary in sorted_titles:
                line = f"{rank}. {cleaned_title}"

                if url:
                    line += f" [URL:{url}]"
                if mobile_url:
                    line += f" [MOBILE:{mobile_url}]"
                if summary:
                    # Replace newlines in summary to keep it on one line
                    clean_summary = summary.replace("\n", " ").replace("\r", "")
                    line += f" [SUMMARY:{clean_summary}]"
                f.write(line + "\n")

            f.write("\n")

        if failed_ids:
            f.write("==== Following IDs Failed ====\n")
            for id_value in failed_ids:
                f.write(f"{id_value}\n")

    return file_path


def load_frequency_words(
    frequency_file: Optional[str] = None,
) -> Tuple[List[Dict], List[str]]:
    """Load frequency words configuration"""
    if frequency_file is None:
        frequency_file = os.environ.get(
            "FREQUENCY_WORDS_PATH", "config/frequency_words.txt"
        )

    frequency_path = Path(frequency_file)
    if not frequency_path.exists():
        raise FileNotFoundError(f"Frequency words file {frequency_file} not found")

    with open(frequency_path, "r", encoding="utf-8") as f:
        content = f.read()

    word_groups = [group.strip() for group in content.split("\n\n") if group.strip()]

    processed_groups = []
    filter_words = []

    for group in word_groups:
        words = [word.strip() for word in group.split("\n") if word.strip()]

        group_required_words = []
        group_normal_words = []
        group_filter_words = []

        for word in words:
            if word.startswith("!"):
                filter_words.append(word[1:])
                group_filter_words.append(word[1:])
            elif word.startswith("+"):
                group_required_words.append(word[1:])
            else:
                group_normal_words.append(word)

        if group_required_words or group_normal_words:
            if group_normal_words:
                group_key = " ".join(group_normal_words)
            else:
                group_key = " ".join(group_required_words)

            processed_groups.append(
                {
                    "required": group_required_words,
                    "normal": group_normal_words,
                    "group_key": group_key,
                }
            )

    return processed_groups, filter_words


def parse_file_titles(file_path: Path) -> Tuple[Dict, Dict]:
    """Parse title data from a single txt file, return (titles_by_id, id_to_name)"""
    titles_by_id = {}
    id_to_name = {}

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        sections = content.split("\n\n")

        for section in sections:
            if not section.strip() or "==== Following IDs Failed ====" in section:
                continue

            lines = section.strip().split("\n")
            if len(lines) < 2:
                continue

            # id | name or id
            header_line = lines[0].strip()
            if " | " in header_line:
                parts = header_line.split(" | ", 1)
                source_id = parts[0].strip()
                name = parts[1].strip()
                id_to_name[source_id] = name
            else:
                source_id = header_line
                id_to_name[source_id] = source_id

            titles_by_id[source_id] = {}

            for line in lines[1:]:
                if line.strip():
                    try:
                        title_part = line.strip()
                        rank = None

                        # Extract rank
                        if ". " in title_part and title_part.split(". ")[0].isdigit():
                            rank_str, title_part = title_part.split(". ", 1)
                            rank = int(rank_str)

                        # Extract MOBILE URL
                        mobile_url = ""
                        if " [MOBILE:" in title_part:
                            title_part, mobile_part = title_part.rsplit(" [MOBILE:", 1)
                            if mobile_part.endswith("]"):
                                mobile_url = mobile_part[:-1]

                        # Extract URL
                        url = ""
                        if " [URL:" in title_part:
                            title_part, url_part = title_part.rsplit(" [URL:", 1)
                            if url_part.endswith("]"):
                                url = url_part[:-1]

                        # Extract SUMMARY
                        summary = ""
                        if " [SUMMARY:" in title_part:
                            title_part, summary_part = title_part.rsplit(" [SUMMARY:", 1)
                            if summary_part.endswith("]"):
                                summary = summary_part[:-1]

                        title = clean_title(title_part.strip())
                        ranks = [rank] if rank is not None else [1]

                        titles_by_id[source_id][title] = {
                            "ranks": ranks,
                            "url": url,
                            "mobileUrl": mobile_url,
                            "summary": summary,
                        }

                    except Exception as e:
                        print(f"Error parsing title line: {line}, error: {e}")

    return titles_by_id, id_to_name


def read_all_today_titles(
    current_platform_ids: Optional[List[str]] = None,
) -> Tuple[Dict, Dict, Dict]:
    """Read all title files for today, support filtering by current monitoring platforms"""
    date_folder = format_date_folder()
    txt_dir = Path("output") / date_folder / "txt"

    if not txt_dir.exists():
        return {}, {}, {}

    all_results = {}
    final_id_to_name = {}
    title_info = {}

    files = sorted([f for f in txt_dir.iterdir() if f.suffix == ".txt"])

    for file_path in files:
        time_info = file_path.stem

        titles_by_id, file_id_to_name = parse_file_titles(file_path)

        if current_platform_ids is not None:
            filtered_titles_by_id = {}
            filtered_id_to_name = {}

            for source_id, title_data in titles_by_id.items():
                if source_id in current_platform_ids:
                    filtered_titles_by_id[source_id] = title_data
                    if source_id in file_id_to_name:
                        filtered_id_to_name[source_id] = file_id_to_name[source_id]

            titles_by_id = filtered_titles_by_id
            file_id_to_name = filtered_id_to_name

        final_id_to_name.update(file_id_to_name)

        for source_id, title_data in titles_by_id.items():
            process_source_data(
                source_id, title_data, time_info, all_results, title_info
            )

    return all_results, final_id_to_name, title_info


def process_source_data(
    source_id: str,
    title_data: Dict,
    time_info: str,
    all_results: Dict,
    title_info: Dict,
) -> None:
    """Process source data, merge duplicate titles"""
    if source_id not in all_results:
        all_results[source_id] = title_data

        if source_id not in title_info:
            title_info[source_id] = {}

        for title, data in title_data.items():
            ranks = data.get("ranks", [])
            url = data.get("url", "")
            mobile_url = data.get("mobileUrl", "")
            summary = data.get("summary", "")

            title_info[source_id][title] = {
                "first_time": time_info,
                "last_time": time_info,
                "count": 1,
                "ranks": ranks,
                "url": url,
                "mobileUrl": mobile_url,
                "summary": summary,
            }
    else:
        for title, data in title_data.items():
            ranks = data.get("ranks", [])
            url = data.get("url", "")
            mobile_url = data.get("mobileUrl", "")
            summary = data.get("summary", "")

            if title not in all_results[source_id]:
                all_results[source_id][title] = {
                    "ranks": ranks,
                    "url": url,
                    "mobileUrl": mobile_url,
                    "summary": summary,
                }
                title_info[source_id][title] = {
                    "first_time": time_info,
                    "last_time": time_info,
                    "count": 1,
                    "ranks": ranks,
                    "url": url,
                    "mobileUrl": mobile_url,
                    "summary": summary,
                }
            else:
                existing_data = all_results[source_id][title]
                existing_ranks = existing_data.get("ranks", [])
                existing_url = existing_data.get("url", "")
                existing_mobile_url = existing_data.get("mobileUrl", "")
                existing_summary = existing_data.get("summary", "")

                merged_ranks = existing_ranks.copy()
                for rank in ranks:
                    if rank not in merged_ranks:
                        merged_ranks.append(rank)

                all_results[source_id][title] = {
                    "ranks": merged_ranks,
                    "url": existing_url or url,
                    "mobileUrl": existing_mobile_url or mobile_url,
                    "summary": existing_summary or summary,
                }

                title_info[source_id][title]["last_time"] = time_info
                title_info[source_id][title]["ranks"] = merged_ranks
                title_info[source_id][title]["count"] += 1
                if not title_info[source_id][title].get("url"):
                    title_info[source_id][title]["url"] = url
                if not title_info[source_id][title].get("mobileUrl"):
                    title_info[source_id][title]["mobileUrl"] = mobile_url
                if not title_info[source_id][title].get("summary"):
                    title_info[source_id][title]["summary"] = summary


def detect_latest_new_titles(
    current_platform_ids: Optional[List[str]] = None,
) -> Dict:
    """Detect new titles in the latest batch of today, support filtering by current monitoring platforms"""
    date_folder = format_date_folder()
    txt_dir = Path("output") / date_folder / "txt"

    if not txt_dir.exists():
        return {}

    files = sorted([f for f in txt_dir.iterdir() if f.suffix == ".txt"])
    if len(files) < 2:
        return {}

    # Parse latest file
    latest_file = files[-1]
    latest_titles, _ = parse_file_titles(latest_file)

    # If current platform list is specified, filter latest file data
    if current_platform_ids is not None:
        filtered_latest_titles = {}
        for source_id, title_data in latest_titles.items():
            if source_id in current_platform_ids:
                filtered_latest_titles[source_id] = title_data
        latest_titles = filtered_latest_titles

    # Aggregate historical titles (filtered by platform)
    historical_titles = {}
    for file_path in files[:-1]:
        historical_data, _ = parse_file_titles(file_path)

        # Filter historical data
        if current_platform_ids is not None:
            filtered_historical_data = {}
            for source_id, title_data in historical_data.items():
                if source_id in current_platform_ids:
                    filtered_historical_data[source_id] = title_data
            historical_data = filtered_historical_data

        for source_id, titles_data in historical_data.items():
            if source_id not in historical_titles:
                historical_titles[source_id] = set()
            for title in titles_data.keys():
                historical_titles[source_id].add(title)

    # Find new titles
    new_titles = {}
    for source_id, latest_source_titles in latest_titles.items():
        historical_set = historical_titles.get(source_id, set())
        source_new_titles = {}

        for title, title_data in latest_source_titles.items():
            if title not in historical_set:
                source_new_titles[title] = title_data

        if source_new_titles:
            new_titles[source_id] = source_new_titles

    return new_titles


# === Statistics and Analysis ===
def calculate_news_weight(
    title_data: Dict, rank_threshold: int = CONFIG["RANK_THRESHOLD"]
) -> float:
    """Calculate news weight for sorting"""
    ranks = title_data.get("ranks", [])
    if not ranks:
        return 0.0

    count = title_data.get("count", len(ranks))
    weight_config = CONFIG["WEIGHT_CONFIG"]

    # Rank weight: sum(11 - min(rank, 10)) / occurrence
    rank_scores = []
    for rank in ranks:
        score = 11 - min(rank, 10)
        rank_scores.append(score)

    rank_weight = sum(rank_scores) / len(ranks) if ranks else 0

    # Frequency weight: min(occurrence, 10) * 10
    frequency_weight = min(count, 10) * 10

    # Hotness bonus: high rank count / total occurrence * 100
    high_rank_count = sum(1 for rank in ranks if rank <= rank_threshold)
    hotness_ratio = high_rank_count / len(ranks) if ranks else 0
    hotness_weight = hotness_ratio * 100

    total_weight = (
        rank_weight * weight_config["RANK_WEIGHT"]
        + frequency_weight * weight_config["FREQUENCY_WEIGHT"]
        + hotness_weight * weight_config["HOTNESS_WEIGHT"]
    )

    return total_weight


def matches_word_groups(
    title: str, word_groups: List[Dict], filter_words: List[str]
) -> bool:
    """Check if title matches word group rules"""
    # If no word groups configured, match all titles (support showing all news)
    if not word_groups:
        return True

    title_lower = title.lower()

    # Filter words check
    if any(filter_word.lower() in title_lower for filter_word in filter_words):
        return False

    # Word group match check
    for group in word_groups:
        required_words = group["required"]
        normal_words = group["normal"]

        # Required words check
        if required_words:
            all_required_present = all(
                req_word.lower() in title_lower for req_word in required_words
            )
            if not all_required_present:
                continue

        # Normal words check
        if normal_words:
            any_normal_present = any(
                normal_word.lower() in title_lower for normal_word in normal_words
            )
            if not any_normal_present:
                continue

        return True

    return False


def format_time_display(first_time: str, last_time: str) -> str:
    """Format time display"""
    if not first_time:
        return ""
    
    # Helper to clean time string
    def clean_time(t):
        return t.replace("时", ":").replace("分", "")

    first_time = clean_time(first_time)
    last_time = clean_time(last_time)

    if first_time == last_time or not last_time:
        return first_time
    else:
        return f"[{first_time} ~ {last_time}]"


def format_rank_display(ranks: List[int], rank_threshold: int, format_type: str) -> str:
    """Unified rank formatting method"""
    if not ranks:
        return ""

    unique_ranks = sorted(set(ranks))
    min_rank = unique_ranks[0]
    max_rank = unique_ranks[-1]

    if format_type == "html":
        highlight_start = "<font color='red'><strong>"
        highlight_end = "</strong></font>"
    elif format_type == "feishu":
        highlight_start = "<font color='red'>**"
        highlight_end = "**</font>"
    elif format_type == "dingtalk":
        highlight_start = "**"
        highlight_end = "**"
    elif format_type == "wework":
        highlight_start = "**"
        highlight_end = "**"
    elif format_type == "telegram":
        highlight_start = "<b>"
        highlight_end = "</b>"
    else:
        highlight_start = "**"
        highlight_end = "**"

    if min_rank <= rank_threshold:
        if min_rank == max_rank:
            return f"{highlight_start}[{min_rank}]{highlight_end}"
        else:
            return f"{highlight_start}[{min_rank} - {max_rank}]{highlight_end}"
    else:
        if min_rank == max_rank:
            return f"[{min_rank}]"
        else:
            return f"[{min_rank} - {max_rank}]"


def count_word_frequency(
    results: Dict,
    word_groups: List[Dict],
    filter_words: List[str],
    id_to_name: Dict,
    title_info: Optional[Dict] = None,
    rank_threshold: int = CONFIG["RANK_THRESHOLD"],
    new_titles: Optional[Dict] = None,
    mode: str = "daily",
) -> Tuple[List[Dict], int]:
    """Count word frequency, support required words, frequency words, filter words, and mark new titles"""

    # If no word groups configured, create a virtual group containing all news
    if not word_groups:
        print("Frequency words configuration is empty, will show all news")
        word_groups = [{"required": [], "normal": [], "group_key": "All News"}]
        filter_words = []  # Clear filter words, show all news

    is_first_today = is_first_crawl_today()

    # Determine data source and new marking logic
    if mode == "incremental":
        if is_first_today:
            # Incremental mode + first time today: process all news, mark all as new
            results_to_process = results
            all_news_are_new = True
        else:
            # Incremental mode + not first time today: only process new news
            results_to_process = new_titles if new_titles else {}
            all_news_are_new = True
    elif mode == "current":
        # current mode: only process current batch news, but stats come from full history
        if title_info:
            latest_time = None
            for source_titles in title_info.values():
                for title_data in source_titles.values():
                    last_time = title_data.get("last_time", "")
                    if last_time:
                        if latest_time is None or last_time > latest_time:
                            latest_time = last_time

            # Only process news with last_time equal to latest time
            if latest_time:
                results_to_process = {}
                for source_id, source_titles in results.items():
                    if source_id in title_info:
                        filtered_titles = {}
                        for title, title_data in source_titles.items():
                            if title in title_info[source_id]:
                                info = title_info[source_id][title]
                                if info.get("last_time") == latest_time:
                                    filtered_titles[title] = title_data
                        if filtered_titles:
                            results_to_process[source_id] = filtered_titles

                print(
                    f"Current ranking mode: Latest time {latest_time}, filtered {sum(len(titles) for titles in results_to_process.values())} current ranking news items"
                )
            else:
                results_to_process = results
        else:
            results_to_process = results
        all_news_are_new = False
    else:
        # Daily summary mode: process all news
        results_to_process = results
        all_news_are_new = False
        total_input_news = sum(len(titles) for titles in results.values())
        filter_status = (
            "Show All"
            if len(word_groups) == 1 and word_groups[0]["group_key"] == "All News"
            else "Frequency Word Filter"
        )
        print(f"Daily summary mode: Processing {total_input_news} items, mode: {filter_status}")

    word_stats = {}
    total_titles = 0
    processed_titles = {}
    matched_new_count = 0

    if title_info is None:
        title_info = {}
    if new_titles is None:
        new_titles = {}

    for group in word_groups:
        group_key = group["group_key"]
        word_stats[group_key] = {"count": 0, "titles": {}}

    for source_id, titles_data in results_to_process.items():
        total_titles += len(titles_data)

        if source_id not in processed_titles:
            processed_titles[source_id] = {}

        for title, title_data in titles_data.items():
            if title in processed_titles.get(source_id, {}):
                continue

            # Use unified matching logic
            matches_frequency_words = matches_word_groups(
                title, word_groups, filter_words
            )

            if not matches_frequency_words:
                continue

            # If incremental mode or current mode first time, count matched new news
            if (mode == "incremental" and all_news_are_new) or (
                mode == "current" and is_first_today
            ):
                matched_new_count += 1

            source_ranks = title_data.get("ranks", [])
            source_url = title_data.get("url", "")
            source_mobile_url = title_data.get("mobileUrl", "")

            # Find matched word group
            title_lower = title.lower()
            for group in word_groups:
                required_words = group["required"]
                normal_words = group["normal"]

                # If "All News" mode, all titles match the first (unique) group
                if len(word_groups) == 1 and word_groups[0]["group_key"] == "All News":
                    group_key = group["group_key"]
                    word_stats[group_key]["count"] += 1
                    if source_id not in word_stats[group_key]["titles"]:
                        word_stats[group_key]["titles"][source_id] = []
                else:
                    # Original matching logic
                    if required_words:
                        all_required_present = all(
                            req_word.lower() in title_lower
                            for req_word in required_words
                        )
                        if not all_required_present:
                            continue

                    if normal_words:
                        any_normal_present = any(
                            normal_word.lower() in title_lower
                            for normal_word in normal_words
                        )
                        if not any_normal_present:
                            continue

                    group_key = group["group_key"]
                    word_stats[group_key]["count"] += 1
                    if source_id not in word_stats[group_key]["titles"]:
                        word_stats[group_key]["titles"][source_id] = []

                first_time = ""
                last_time = ""
                count_info = 1
                ranks = source_ranks if source_ranks else []
                url = source_url
                mobile_url = source_mobile_url

                # For current mode, get full data from historical stats
                if (
                    mode == "current"
                    and title_info
                    and source_id in title_info
                    and title in title_info[source_id]
                ):
                    info = title_info[source_id][title]
                    first_time = info.get("first_time", "")
                    last_time = info.get("last_time", "")
                    count_info = info.get("count", 1)
                    if "ranks" in info and info["ranks"]:
                        ranks = info["ranks"]
                    url = info.get("url", source_url)
                    mobile_url = info.get("mobileUrl", source_mobile_url)
                elif (
                    title_info
                    and source_id in title_info
                    and title in title_info[source_id]
                ):
                    info = title_info[source_id][title]
                    first_time = info.get("first_time", "")
                    last_time = info.get("last_time", "")
                    count_info = info.get("count", 1)
                    if "ranks" in info and info["ranks"]:
                        ranks = info["ranks"]
                    url = info.get("url", source_url)
                    mobile_url = info.get("mobileUrl", source_mobile_url)

                if not ranks:
                    ranks = [99]

                time_display = format_time_display(first_time, last_time)

                source_name = id_to_name.get(source_id, source_id)

                # Check if new
                is_new = False
                if all_news_are_new:
                    # In incremental mode, all processed news are new, or all news on first run are new
                    is_new = True
                elif new_titles and source_id in new_titles:
                    # Check if in new list
                    new_titles_for_source = new_titles[source_id]
                    is_new = title in new_titles_for_source

                word_stats[group_key]["titles"][source_id].append(
                    {
                        "title": title,
                        "source_name": source_name,
                        "first_time": first_time,
                        "last_time": last_time,
                        "time_display": time_display,
                        "count": count_info,
                        "ranks": ranks,
                        "rank_threshold": rank_threshold,
                        "url": url,
                        "mobileUrl": mobile_url,
                        "is_new": is_new,
                    }
                )

                if source_id not in processed_titles:
                    processed_titles[source_id] = {}
                processed_titles[source_id][title] = True

                break

    # Finally print summary information
    if mode == "incremental":
        if is_first_today:
            total_input_news = sum(len(titles) for titles in results.values())
            filter_status = (
                "Show All"
                if len(word_groups) == 1 and word_groups[0]["group_key"] == "All News"
                else "Keyword Matched"
            )
            print(
                f"Incremental mode: First crawl today, {total_input_news} news items, {matched_new_count} {filter_status}"
            )
        else:
            if new_titles:
                total_new_count = sum(len(titles) for titles in new_titles.values())
                filter_status = (
                    "Show All"
                    if len(word_groups) == 1
                    and word_groups[0]["group_key"] == "All News"
                    else "Keyword Matched"
                )
                print(
                    f"Incremental mode: {total_new_count} new news items, {matched_new_count} {filter_status}"
                )
                if matched_new_count == 0 and len(word_groups) > 1:
                    print("Incremental mode: No new news matched keywords, will not send notification")
            else:
                print("Incremental mode: No new news detected")
    elif mode == "current":
        total_input_news = sum(len(titles) for titles in results_to_process.values())
        if is_first_today:
            filter_status = (
                "Show All"
                if len(word_groups) == 1 and word_groups[0]["group_key"] == "All News"
                else "Keyword Matched"
            )
            print(
                f"Current ranking mode: First crawl today, {total_input_news} current ranking news items, {matched_new_count} {filter_status}"
            )
        else:
            matched_count = sum(stat["count"] for stat in word_stats.values())
            filter_status = (
                "Show All"
                if len(word_groups) == 1 and word_groups[0]["group_key"] == "All News"
                else "Keyword Matched"
            )
            print(
                f"Current ranking mode: {total_input_news} current ranking news items, {matched_count} {filter_status}"
            )

    stats = []
    for group_key, data in word_stats.items():
        all_titles = []
        for source_id, title_list in data["titles"].items():
            all_titles.extend(title_list)

        # Sort by weight
        sorted_titles = sorted(
            all_titles,
            key=lambda x: (
                -calculate_news_weight(x, rank_threshold),
                min(x["ranks"]) if x["ranks"] else 999,
                -x["count"],
            ),
        )

        stats.append(
            {
                "word": group_key,
                "count": data["count"],
                "titles": sorted_titles,
                "percentage": (
                    round(data["count"] / total_titles * 100, 2)
                    if total_titles > 0
                    else 0
                ),
            }
        )

    stats.sort(key=lambda x: x["count"], reverse=True)
    return stats, total_titles


# === Report Generation ===
def prepare_report_data(
    stats: List[Dict],
    failed_ids: Optional[List] = None,
    new_titles: Optional[Dict] = None,
    id_to_name: Optional[Dict] = None,
    mode: str = "daily",
) -> Dict:
    """Prepare report data"""
    processed_new_titles = []

    # Hide new news section in incremental mode
    hide_new_section = mode == "incremental"

    # Only process new news section if not hidden
    if not hide_new_section:
        filtered_new_titles = {}
        if new_titles and id_to_name:
            word_groups, filter_words = load_frequency_words()
            for source_id, titles_data in new_titles.items():
                filtered_titles = {}
                for title, title_data in titles_data.items():
                    if matches_word_groups(title, word_groups, filter_words):
                        filtered_titles[title] = title_data
                if filtered_titles:
                    filtered_new_titles[source_id] = filtered_titles

        if filtered_new_titles and id_to_name:
            for source_id, titles_data in filtered_new_titles.items():
                source_name = id_to_name.get(source_id, source_id)
                source_titles = []

                for title, title_data in titles_data.items():
                    url = title_data.get("url", "")
                    mobile_url = title_data.get("mobileUrl", "")
                    ranks = title_data.get("ranks", [])

                    processed_title = {
                        "title": title,
                        "source_name": source_name,
                        "time_display": "",
                        "count": 1,
                        "ranks": ranks,
                        "rank_threshold": CONFIG["RANK_THRESHOLD"],
                        "url": url,
                        "mobile_url": mobile_url,
                        "is_new": True,
                    }
                    source_titles.append(processed_title)

                if source_titles:
                    processed_new_titles.append(
                        {
                            "source_id": source_id,
                            "source_name": source_name,
                            "titles": source_titles,
                        }
                    )

    processed_stats = []
    for stat in stats:
        if stat["count"] <= 0:
            continue

        processed_titles = []
        for title_data in stat["titles"]:
            processed_title = {
                "title": title_data["title"],
                "source_name": title_data["source_name"],
                "time_display": title_data["time_display"],
                "count": title_data["count"],
                "ranks": title_data["ranks"],
                "rank_threshold": title_data["rank_threshold"],
                "url": title_data.get("url", ""),
                "mobile_url": title_data.get("mobileUrl", ""),
                "is_new": title_data.get("is_new", False),
            }
            processed_titles.append(processed_title)

        processed_stats.append(
            {
                "word": stat["word"],
                "count": stat["count"],
                "percentage": stat.get("percentage", 0),
                "titles": processed_titles,
            }
        )

    return {
        "stats": processed_stats,
        "new_titles": processed_new_titles,
        "failed_ids": failed_ids or [],
        "total_new_count": sum(
            len(source["titles"]) for source in processed_new_titles
        ),
    }


def format_title_for_platform(
    platform: str, title_data: Dict, show_source: bool = True
) -> str:
    """Unified title formatting method"""
    rank_display = format_rank_display(
        title_data["ranks"], title_data["rank_threshold"], platform
    )

    link_url = title_data["mobile_url"] or title_data["url"]

    cleaned_title = clean_title(title_data["title"])

    if platform == "feishu":
        if link_url:
            formatted_title = f"[{cleaned_title}]({link_url})"
        else:
            formatted_title = cleaned_title

        title_prefix = "🆕 " if title_data.get("is_new") else ""

        if show_source:
            result = f"<font color='grey'>[{title_data['source_name']}]</font> {title_prefix}{formatted_title}"
        else:
            result = f"{title_prefix}{formatted_title}"

        if rank_display:
            result += f" {rank_display}"
        if title_data["time_display"]:
            result += f" <font color='grey'>- {title_data['time_display']}</font>"
        if title_data["count"] > 1:
            result += f" <font color='green'>({title_data['count']} times)</font>"

        return result

    elif platform == "dingtalk":
        if link_url:
            formatted_title = f"[{cleaned_title}]({link_url})"
        else:
            formatted_title = cleaned_title

        title_prefix = "🆕 " if title_data.get("is_new") else ""

        if show_source:
            result = f"[{title_data['source_name']}] {title_prefix}{formatted_title}"
        else:
            result = f"{title_prefix}{formatted_title}"

        if rank_display:
            result += f" {rank_display}"
        if title_data["time_display"]:
            result += f" - {title_data['time_display']}"
        if title_data["count"] > 1:
            result += f" ({title_data['count']} times)"

        return result

    elif platform == "wework":
        if link_url:
            formatted_title = f"[{cleaned_title}]({link_url})"
        else:
            formatted_title = cleaned_title

        title_prefix = "🆕 " if title_data.get("is_new") else ""

        if show_source:
            result = f"[{title_data['source_name']}] {title_prefix}{formatted_title}"
        else:
            result = f"{title_prefix}{formatted_title}"

        if rank_display:
            result += f" {rank_display}"
        if title_data["time_display"]:
            result += f" - {title_data['time_display']}"
        if title_data["count"] > 1:
            result += f" ({title_data['count']} times)"

        return result

    elif platform == "telegram":
        if link_url:
            formatted_title = f'<a href="{link_url}">{html_escape(cleaned_title)}</a>'
        else:
            formatted_title = cleaned_title

        title_prefix = "🆕 " if title_data.get("is_new") else ""

        if show_source:
            result = f"[{title_data['source_name']}] {title_prefix}{formatted_title}"
        else:
            result = f"{title_prefix}{formatted_title}"

        if rank_display:
            result += f" {rank_display}"
        if title_data["time_display"]:
            result += f" <code>- {title_data['time_display']}</code>"
        if title_data["count"] > 1:
            result += f" <code>({title_data['count']} times)</code>"

        return result

    elif platform == "ntfy":
        if link_url:
            formatted_title = f"[{cleaned_title}]({link_url})"
        else:
            formatted_title = cleaned_title

        title_prefix = "🆕 " if title_data.get("is_new") else ""

        if show_source:
            result = f"[{title_data['source_name']}] {title_prefix}{formatted_title}"
        else:
            result = f"{title_prefix}{formatted_title}"

        if rank_display:
            result += f" {rank_display}"
        if title_data["time_display"]:
            result += f" `- {title_data['time_display']}`"
        if title_data["count"] > 1:
            result += f" `({title_data['count']} times)`"

        return result

    elif platform == "html":
        rank_display = format_rank_display(
            title_data["ranks"], title_data["rank_threshold"], "html"
        )

        link_url = title_data["mobile_url"] or title_data["url"]

        escaped_title = html_escape(cleaned_title)
        escaped_source_name = html_escape(title_data["source_name"])

        if link_url:
            escaped_url = html_escape(link_url)
            formatted_title = f'[{escaped_source_name}] <a href="{escaped_url}" target="_blank" class="news-link">{escaped_title}</a>'
        else:
            formatted_title = (
                f'[{escaped_source_name}] <span class="no-link">{escaped_title}</span>'
            )

        if rank_display:
            formatted_title += f" {rank_display}"
        if title_data["time_display"]:
            escaped_time = html_escape(title_data["time_display"])
            formatted_title += f" <font color='grey'>- {escaped_time}</font>"
        if title_data["count"] > 1:
            formatted_title += f" <font color='green'>({title_data['count']} times)</font>"

        if title_data.get("is_new"):
            formatted_title = f"<div class='new-title'>🆕 {formatted_title}</div>"

        return formatted_title

    else:
        return cleaned_title


def generate_html_report(
    stats: List[Dict],
    total_titles: int,
    failed_ids: Optional[List] = None,
    new_titles: Optional[Dict] = None,
    id_to_name: Optional[Dict] = None,
    mode: str = "daily",
    is_daily_summary: bool = False,
    update_info: Optional[Dict] = None,
) -> str:
    """Generate HTML report"""
    if is_daily_summary:
        if mode == "current":
            filename = "current_ranking.html"
        elif mode == "incremental":
            filename = "daily_incremental.html"
        else: # mode == "daily" or default
            filename = "daily_summary.html"
    else: # Not a daily summary, so it's a real-time report
        filename = f"{format_time_filename()}.html"

    file_path = get_output_path("html", filename)

    report_data = prepare_report_data(stats, failed_ids, new_titles, id_to_name, mode)

    html_content = render_html_content(
        report_data, total_titles, is_daily_summary, mode, update_info
    )

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    if is_daily_summary:
        root_file_path = Path("index.html")
        with open(root_file_path, "w", encoding="utf-8") as f:
            f.write(html_content)

    # Generate email-optimized version
    email_html_content = render_email_template(
        report_data, total_titles, is_daily_summary, mode, update_info
    )
    email_file_path = file_path.replace(".html", "_email.html")
    with open(email_file_path, "w", encoding="utf-8") as f:
        f.write(email_html_content)

    return file_path


def render_html_content(
    report_data: Dict,
    total_titles: int,
    is_daily_summary: bool = False,
    mode: str = "daily",
    update_info: Optional[Dict] = None,
) -> str:
    """Render HTML content with a premium, modern design"""
    
    # Calculate stats for the header
    hot_news_count = sum(len(stat["titles"]) for stat in report_data["stats"])
    now = get_beijing_time()
    generated_time = now.strftime("%b %d, %H:%M")
    
    report_type_map = {
        "daily": "Daily Summary",
        "current": "Current Ranking",
        "incremental": "Incremental Alert"
    }
    report_type_display = report_type_map.get(mode, "Trend Report")
    if not is_daily_summary:
        report_type_display = "Real-time Analysis"

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>TrendRadar Report</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
        <style>
            :root {{
                --primary: #2563eb;
                --primary-dark: #1e40af;
                --bg-color: #f3f4f6;
                --card-bg: #ffffff;
                --text-main: #111827;
                --text-secondary: #6b7280;
                --border-color: #e5e7eb;
                --accent-hot: #ef4444;
                --accent-warm: #f97316;
                --accent-new: #10b981;
            }}
            
            body {{
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background-color: var(--bg-color);
                color: var(--text-main);
                margin: 0;
                padding: 20px;
                line-height: 1.5;
                -webkit-font-smoothing: antialiased;
            }}
            
            .container {{
                max-width: 680px;
                margin: 0 auto;
                background: var(--card-bg);
                border-radius: 16px;
                overflow: hidden;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            }}
            
            /* Header Section */
            .header {{
                background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
                color: white;
                padding: 40px 32px;
                position: relative;
            }}
            
            .brand {{
                font-size: 14px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1px;
                opacity: 0.8;
                margin-bottom: 12px;
                display: block;
            }}
            
            .title {{
                font-size: 28px;
                font-weight: 800;
                margin: 0;
                letter-spacing: -0.5px;
                line-height: 1.2;
            }}
            
            .subtitle {{
                font-size: 16px;
                opacity: 0.9;
                margin-top: 8px;
                font-weight: 400;
            }}
            
            /* Stats Grid */
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 1px;
                background: var(--border-color);
                border-bottom: 1px solid var(--border-color);
            }}
            
            .stat-item {{
                background: var(--card-bg);
                padding: 20px;
                text-align: center;
            }}
            
            .stat-value {{
                display: block;
                font-size: 24px;
                font-weight: 700;
                color: var(--primary);
                margin-bottom: 4px;
            }}
            
            .stat-label {{
                font-size: 12px;
                color: var(--text-secondary);
                text-transform: uppercase;
                letter-spacing: 0.5px;
                font-weight: 600;
            }}
            
            /* Content Area */
            .content {{
                padding: 32px;
            }}
            
            /* Section Headers */
            .section-header {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                margin-bottom: 24px;
                padding-bottom: 12px;
                border-bottom: 2px solid var(--bg-color);
            }}
            
            .section-title {{
                font-size: 18px;
                font-weight: 700;
                color: var(--text-main);
                display: flex;
                align-items: center;
                gap: 8px;
            }}
            
            .section-badge {{
                background: var(--bg-color);
                padding: 4px 10px;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
                color: var(--text-secondary);
            }}
            
            /* Word Groups */
            .word-group {{
                margin-bottom: 40px;
            }}
            
            .word-header {{
                display: flex;
                align-items: center;
                margin-bottom: 16px;
            }}
            
            .keyword {{
                font-size: 20px;
                font-weight: 700;
                color: var(--text-main);
                margin-right: 12px;
            }}
            
            .keyword-count {{
                font-size: 13px;
                font-weight: 600;
                padding: 2px 8px;
                border-radius: 6px;
                background: var(--bg-color);
                color: var(--text-secondary);
            }}
            
            .keyword-count.hot {{ background: #fee2e2; color: var(--accent-hot); }}
            .keyword-count.warm {{ background: #ffedd5; color: var(--accent-warm); }}
            
            /* News Items */
            .news-list {{
                display: flex;
                flex-direction: column;
                gap: 16px;
            }}
            
            .news-card {{
                background: white;
                border: 1px solid var(--border-color);
                border-radius: 12px;
                padding: 16px;
                transition: transform 0.2s, box-shadow 0.2s;
                position: relative;
                text-decoration: none;
                display: block;
            }}
            
            .news-card:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.05);
                border-color: var(--primary);
            }}
            
            .news-meta {{
                display: flex;
                align-items: center;
                gap: 8px;
                margin-bottom: 10px;
                font-size: 12px;
                flex-wrap: wrap;
            }}
            
            .source-badge {{
                background: var(--bg-color);
                color: var(--text-main);
                padding: 2px 8px;
                border-radius: 4px;
                font-weight: 600;
            }}
            
            .rank-badge {{
                background: var(--text-main);
                color: white;
                padding: 2px 6px;
                border-radius: 4px;
                font-weight: 700;
            }}
            
            .rank-badge.top {{ background: var(--accent-hot); }}
            .rank-badge.high {{ background: var(--accent-warm); }}
            
            .time-badge {{
                color: var(--text-secondary);
                display: flex;
                align-items: center;
                gap: 4px;
            }}
            
            .news-title {{
                font-size: 16px;
                font-weight: 600;
                color: var(--text-main);
                line-height: 1.4;
                margin: 0;
            }}
            
            .news-card:visited .news-title {{
                color: #4b5563;
            }}
            
            .new-badge {{
                position: absolute;
                top: -8px;
                right: -8px;
                background: var(--accent-new);
                color: white;
                font-size: 10px;
                font-weight: 700;
                padding: 4px 8px;
                border-radius: 12px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                text-transform: uppercase;
            }}
            
            /* Footer */
            .footer {{
                background: var(--bg-color);
                padding: 32px;
                text-align: center;
                border-top: 1px solid var(--border-color);
            }}
            
            .footer-text {{
                font-size: 13px;
                color: var(--text-secondary);
                margin-bottom: 8px;
            }}
            
            .footer-link {{
                color: var(--primary);
                text-decoration: none;
                font-weight: 600;
            }}
            
            .version-info {{
                display: inline-block;
                margin-top: 12px;
                padding: 4px 12px;
                background: #fff;
                border: 1px solid var(--border-color);
                border-radius: 20px;
                font-size: 12px;
                color: var(--text-secondary);
            }}
            
            /* Save Buttons */
            .action-bar {{
                position: absolute;
                top: 20px;
                right: 20px;
                display: flex;
                gap: 8px;
            }}
            
            .btn {{
                background: rgba(255,255,255,0.1);
                border: 1px solid rgba(255,255,255,0.2);
                color: white;
                padding: 6px 12px;
                border-radius: 6px;
                font-size: 12px;
                font-weight: 500;
                cursor: pointer;
                backdrop-filter: blur(4px);
                transition: background 0.2s;
            }}
            
            .btn:hover {{
                background: rgba(255,255,255,0.2);
            }}
            
            @media (max-width: 480px) {{
                body {{ padding: 0; }}
                .container {{ border-radius: 0; }}
                .stats-grid {{ grid-template-columns: 1fr; }}
                .stat-item {{ border-bottom: 1px solid var(--border-color); }}
                .header {{ padding: 32px 20px; }}
                .content {{ padding: 20px; }}
                .action-bar {{ position: static; margin-bottom: 20px; justify-content: center; }}
                .btn {{ background: rgba(255,255,255,0.15); color: white; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="action-bar">
                    <button class="btn" onclick="saveAsImage()">Save Image</button>
                    <button class="btn" onclick="saveAsMultipleImages()">Save Segments</button>
                </div>
                <span class="brand">TrendRadar AI</span>
                <h1 class="title">{report_type_display}</h1>
                <div class="subtitle">{generated_time}</div>
            </div>
            
            <div class="stats-grid">
                <div class="stat-item">
                    <span class="stat-value">{total_titles}</span>
                    <span class="stat-label">Total News</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{hot_news_count}</span>
                    <span class="stat-label">Trending</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{len(report_data.get('new_titles', []))}</span>
                    <span class="stat-label">New Sources</span>
                </div>
            </div>
            
            <div class="content">
    """

    # Failed IDs Section
    if report_data["failed_ids"]:
        html += """
        <div style="background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 16px; margin-bottom: 32px;">
            <h3 style="color: #991b1b; margin: 0 0 8px 0; font-size: 14px;">⚠️ Connection Failed</h3>
            <div style="color: #b91c1c; font-size: 13px;">
        """
        html += ", ".join([html_escape(pid) for pid in report_data["failed_ids"]])
        html += """
            </div>
        </div>
        """

    # Main Stats Section
    if report_data["stats"]:
        html += """
        <div class="section-header">
            <div class="section-title">
                <span>🔥 Trending Topics</span>
            </div>
        </div>
        """
        
        for i, stat in enumerate(report_data["stats"], 1):
            count = stat["count"]
            count_class = "hot" if count >= 10 else "warm" if count >= 5 else ""
            escaped_word = html_escape(stat["word"])
            
            html += f"""
            <div class="word-group">
                <div class="word-header">
                    <span class="keyword">{escaped_word}</span>
                    <span class="keyword-count {count_class}">{count} items</span>
                </div>
                <div class="news-list">
            """
            
            for title_data in stat["titles"]:
                # Extract data
                title = html_escape(title_data["title"])
                source = html_escape(title_data["source_name"])
                url = title_data.get("mobile_url") or title_data.get("url", "#")
                is_new = title_data.get("is_new", False)
                
                # Ranks
                ranks = title_data.get("ranks", [])
                rank_html = ""
                if ranks:
                    min_rank = min(ranks)
                    rank_class = "top" if min_rank <= 3 else "high" if min_rank <= 10 else ""
                    rank_text = f"#{min_rank}" if min(ranks) == max(ranks) else f"#{min(ranks)}-{max(ranks)}"
                    rank_html = f'<span class="rank-badge {rank_class}">{rank_text}</span>'
                
                # Time
                time_display = title_data.get("time_display", "")
                time_html = ""
                if time_display:
                    clean_time = time_display.replace("[", "").replace("]", "")
                    time_html = f'<span class="time-badge">🕒 {clean_time}</span>'
                
                # New Badge
                new_badge = '<span class="new-badge">NEW</span>' if is_new else ''
                
                html += f"""
                <a href="{url}" target="_blank" class="news-card">
                    {new_badge}
                    <div class="news-meta">
                        <span class="source-badge">{source}</span>
                        {rank_html}
                        {time_html}
                    </div>
                    <h3 class="news-title">{title}</h3>
                </a>
                """
            
            html += """
                </div>
            </div>
            """

    # New News Section
    if report_data["new_titles"]:
        total_new = report_data['total_new_count']
        html += f"""
        <div class="section-header" style="margin-top: 48px;">
            <div class="section-title">
                <span>⚡ New Discoveries</span>
                <span class="section-badge">{total_new}</span>
            </div>
        </div>
        """
        
        for source_data in report_data["new_titles"]:
            source_name = html_escape(source_data["source_name"])
            
            html += f"""
            <div class="word-group">
                <div class="word-header">
                    <span class="keyword" style="font-size: 16px;">{source_name}</span>
                </div>
                <div class="news-list">
            """
            
            for title_data in source_data["titles"]:
                title = html_escape(title_data["title"])
                url = title_data.get("mobile_url") or title_data.get("url", "#")
                
                # Ranks for new items
                ranks = title_data.get("ranks", [])
                rank_html = ""
                if ranks:
                    min_rank = min(ranks)
                    rank_class = "top" if min_rank <= 3 else "high" if min_rank <= 10 else ""
                    rank_text = f"#{min_rank}"
                    rank_html = f'<span class="rank-badge {rank_class}">{rank_text}</span>'
                
                html += f"""
                <a href="{url}" target="_blank" class="news-card">
                    <div class="news-meta">
                        {rank_html}
                        <span class="time-badge">Just now</span>
                    </div>
                    <h3 class="news-title">{title}</h3>
                </a>
                """
                
            html += """
                </div>
            </div>
            """

    # Footer
    html += """
            </div>
            <div class="footer">
                <div class="footer-text">
                    Generated by <a href="https://github.com/sansan0/TrendRadar" class="footer-link">TrendRadar</a>
                </div>
    """
    
    if update_info:
        html += f"""
                <div class="version-info">
                    Update Available: {update_info['remote_version']}
                </div>
        """
        
    html += """
            </div>
        </div>
        
        <script>
            // Simple save functionality
            async function saveAsImage() {
                const btn = event.target;
                const originalText = btn.textContent;
                btn.textContent = 'Saving...';
                
                try {
                    const container = document.querySelector('.container');
                    const canvas = await html2canvas(container, {
                        scale: 2,
                        backgroundColor: null,
                        useCORS: true
                    });
                    
                    const link = document.createElement('a');
                    link.download = `TrendRadar-Report-${new Date().getTime()}.png`;
                    link.href = canvas.toDataURL('image/png');
                    link.click();
                    
                    btn.textContent = 'Saved!';
                } catch (e) {
                    console.error(e);
                    btn.textContent = 'Error';
                }
                
                setTimeout(() => btn.textContent = originalText, 2000);
            }
            
            // Placeholder for segment save if needed, simplified for now
            async function saveAsMultipleImages() {
                alert('Segment save not implemented in this simplified template.');
            }
        </script>
    </body>
    </html>
    """
    
    return html


def render_email_template(
    report_data: Dict,
    total_titles: int,
    is_daily_summary: bool = False,
    mode: str = "daily",
    update_info: Optional[Dict] = None,
) -> str:
    """Render email-friendly HTML content (Table-based, Inline CSS)"""
    
    # Calculate stats
    hot_news_count = sum(len(stat["titles"]) for stat in report_data["stats"])
    now = get_beijing_time()
    generated_time = now.strftime("%b %d, %H:%M")
    
    report_type_map = {
        "daily": "Daily Summary",
        "current": "Current Ranking",
        "incremental": "Incremental Alert"
    }
    report_type_display = report_type_map.get(mode, "Trend Report")
    if not is_daily_summary:
        report_type_display = "Real-time Analysis"

    # Colors
    c_bg = "#f3f4f6"
    c_card = "#ffffff"
    c_text = "#111827"
    c_text_light = "#6b7280"
    c_primary = "#2563eb"
    c_border = "#e5e7eb"
    
    html = f"""
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
        <title>TrendRadar Report</title>
    </head>
    <body style="margin: 0; padding: 0; background-color: {c_bg}; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <table border="0" cellpadding="0" cellspacing="0" width="100%" style="background-color: {c_bg};">
            <tr>
                <td align="center" style="padding: 20px 0;">
                    <!-- Main Container -->
                    <table border="0" cellpadding="0" cellspacing="0" width="600" style="background-color: {c_card}; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05);">
                        
                        <!-- Header -->
                        <tr>
                            <td style="background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); padding: 30px; color: #ffffff; text-align: center;">
                                <div style="font-size: 12px; text-transform: uppercase; letter-spacing: 1px; opacity: 0.8; margin-bottom: 10px;">TrendRadar AI</div>
                                <h1 style="margin: 0; font-size: 24px; font-weight: 800; letter-spacing: -0.5px;">{report_type_display}</h1>
                                <div style="margin-top: 5px; font-size: 14px; opacity: 0.9;">{generated_time}</div>
                            </td>
                        </tr>
                        
                        <!-- Stats Bar -->
                        <tr>
                            <td style="border-bottom: 1px solid {c_border};">
                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                    <tr>
                                        <td width="33%" align="center" style="padding: 15px; border-right: 1px solid {c_border};">
                                            <div style="font-size: 20px; font-weight: 700; color: {c_primary};">{total_titles}</div>
                                            <div style="font-size: 10px; text-transform: uppercase; color: {c_text_light}; font-weight: 600;">Total News</div>
                                        </td>
                                        <td width="33%" align="center" style="padding: 15px; border-right: 1px solid {c_border};">
                                            <div style="font-size: 20px; font-weight: 700; color: {c_primary};">{hot_news_count}</div>
                                            <div style="font-size: 10px; text-transform: uppercase; color: {c_text_light}; font-weight: 600;">Trending</div>
                                        </td>
                                        <td width="33%" align="center" style="padding: 15px;">
                                            <div style="font-size: 20px; font-weight: 700; color: {c_primary};">{len(report_data.get('new_titles', []))}</div>
                                            <div style="font-size: 10px; text-transform: uppercase; color: {c_text_light}; font-weight: 600;">New Sources</div>
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                        
                        <!-- Content -->
                        <tr>
                            <td style="padding: 30px;">
    """
    
    # Failed IDs
    if report_data["failed_ids"]:
        failed_list = ", ".join([html_escape(pid) for pid in report_data["failed_ids"]])
        html += f"""
        <div style="background-color: #fef2f2; border: 1px solid #fecaca; border-radius: 6px; padding: 15px; margin-bottom: 25px;">
            <div style="color: #991b1b; font-weight: 600; margin-bottom: 5px; display: flex; align-items: center;">
                <span style="margin-right: 6px;">⚠️</span> Failed Sources
            </div>
            <div style="color: #b91c1c; font-size: 13px;">{failed_list}</div>
        </div>
        """

    # New Titles (Prioritize displaying new items)
    if report_data["new_titles"]:
        html += f"""
        <div style="margin-bottom: 30px;">
            <div style="display: flex; align-items: center; margin-bottom: 20px;">
                <div style="background: {c_primary}; width: 4px; height: 24px; border-radius: 2px; margin-right: 12px;"></div>
                <h2 style="margin: 0; font-size: 18px; color: {c_text};">Latest Updates</h2>
            </div>
        """
        
        # Collect titles already shown
        shown_titles = set()
        for source_data in report_data["new_titles"]:
            source_name = html_escape(source_data["source_name"])
            html += f"""
            <div style="margin-bottom: 25px;">
                <div style="font-size: 14px; font-weight: 700; color: {c_text_light}; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px; border-bottom: 1px solid {c_border}; padding-bottom: 8px;">
                    {source_name}
                </div>
            """
            
            for title_data in source_data["titles"]:
                title = html_escape(title_data["title"])
                url = title_data.get("mobile_url") or title_data.get("url")
                summary = title_data.get("summary", "")
                shown_titles.add(title_data["title"]) # Add original title to set
                
                html += f"""
                <div style="margin-bottom: 16px; padding-bottom: 16px; border-bottom: 1px solid #f3f4f6;">
                    <div style="font-size: 16px; font-weight: 600; line-height: 1.4; margin-bottom: 6px;">
                """
                
                if url:
                    html += f'<a href="{html_escape(url)}" style="text-decoration: none; color: {c_text}; hover: color: {c_primary};">{title}</a>'
                else:
                    html += f'<span style="color: {c_text};">{title}</span>'
                    
                html += "</div>"
                
                if summary:
                    clean_summary = html_escape(summary).replace("\n", " ")
                    html += f"""
                    <div style="font-size: 14px; color: #4b5563; line-height: 1.6; margin-bottom: 8px;">
                        {clean_summary}
                    </div>
                    """
                
                # Metadata line
                html += f"""
                    <div style="font-size: 12px; color: #9ca3af; display: flex; align-items: center;">
                        <span style="background-color: #e0f2fe; color: #0369a1; padding: 2px 6px; border-radius: 4px; font-weight: 500; font-size: 11px;">NEW</span>
                        {f'<span style="margin-left: 10px;">🔗 <a href="{html_escape(url)}" style="color: {c_primary}; text-decoration: none;">Read more</a></span>' if url else ''}
                    </div>
                </div>
                """
            html += "</div>"
        html += "</div>"

    # Trending Stats (Simplified or Hidden based on user preference, but keeping it for now as secondary)
    # User requested "I can see all keywords maybe that shouldbt be visible"
    # So we will hide the detailed keyword stats list and only show news items grouped by keyword if they are NOT already shown in "New Titles"
    
    # Collect titles already shown (moved up to be available for both sections)
    # shown_titles = set()
    # for source in report_data.get("new_titles", []):
    #     for t in source["titles"]:
    #         shown_titles.add(t["title"])

    if report_data["stats"]:
        has_other_news = False
        # Check if there are news not yet shown
        for stat in report_data["stats"]:
            for t in stat["titles"]:
                if t["title"] not in shown_titles:
                    has_other_news = True
                    break
            if has_other_news:
                break
        
        if has_other_news:
            html += f"""
            <div style="margin-top: 40px;">
                <div style="display: flex; align-items: center; margin-bottom: 20px;">
                    <div style="background: #8b5cf6; width: 4px; height: 24px; border-radius: 2px; margin-right: 12px;"></div>
                    <h2 style="margin: 0; font-size: 18px; color: {c_text};">Trending Topics</h2>
                </div>
            """
            
            for stat in report_data["stats"]:
                # Skip if all titles in this group are already shown
                group_titles = [t for t in stat["titles"] if t["title"] not in shown_titles]
                if not group_titles:
                    continue
                    
                word = html_escape(stat["word"])
                count = stat["count"]
                
                html += f"""
                <div style="margin-bottom: 30px;">
                    <div style="background-color: #f8fafc; padding: 8px 12px; border-radius: 6px; margin-bottom: 15px; display: inline-block;">
                        <span style="font-weight: 700; color: #475569;">#{word}</span>
                        <span style="font-size: 12px; color: #94a3b8; margin-left: 6px;">{count} items</span>
                    </div>
                """
                
                for title_data in group_titles:
                    title = html_escape(title_data["title"])
                    url = title_data.get("mobile_url") or title_data.get("url")
                    summary = title_data.get("summary", "")
                    
                    html += f"""
                    <div style="margin-bottom: 16px; padding-bottom: 16px; border-bottom: 1px solid #f3f4f6;">
                        <div style="font-size: 15px; font-weight: 600; line-height: 1.4; margin-bottom: 4px;">
                    """
                    
                    if url:
                        html += f'<a href="{html_escape(url)}" style="text-decoration: none; color: {c_text};">{title}</a>'
                    else:
                        html += f'<span style="color: {c_text};">{title}</span>'
                        
                    html += "</div>"
                    
                    if summary:
                        clean_summary = html_escape(summary).replace("\n", " ")
                        html += f"""
                        <div style="font-size: 13px; color: #4b5563; line-height: 1.5; margin-bottom: 6px;">
                            {clean_summary}
                        </div>
                        """
                        
                    if url:
                        html += f"""
                        <div style="font-size: 12px;">
                            <a href="{html_escape(url)}" style="color: {c_primary}; text-decoration: none;">Read more →</a>
                        </div>
                        """
                    html += "</div>"
                html += "</div>"
            html += "</div>"

    # Empty State
    if not report_data["stats"] and not report_data["new_titles"] and not report_data["failed_ids"]:
        html += f"""
        <div style="text-align: center; padding: 40px 20px; color: {c_text_light};">
            <div style="font-size: 48px; margin-bottom: 15px;">📭</div>
            <div style="font-size: 16px; font-weight: 500;">No new updates found</div>
            <div style="font-size: 14px; margin-top: 5px;">We'll keep looking for you.</div>
        </div>
        """

    html += f"""
                            </td>
                        </tr>
                        
                        <!-- Footer -->
                        <tr>
                            <td style="background-color: {c_bg}; padding: 20px; text-align: center; border-top: 1px solid {c_border};">
                                <div style="font-size: 12px; color: {c_text_light}; margin-bottom: 5px;">Generated by <a href="https://github.com/sansan0/TrendRadar" style="color: {c_primary}; text-decoration: none; font-weight: 600;">TrendRadar</a></div>
    """
    
    if update_info:
        html += f"""
                                <div style="display: inline-block; margin-top: 8px; padding: 4px 10px; background-color: #ffffff; border: 1px solid {c_border}; border-radius: 12px; font-size: 11px; color: {c_text_light};">
                                    Update Available: {update_info['remote_version']}
                                </div>
        """
        
    html += """
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    
    return html


def render_feishu_content(
    report_data: Dict, update_info: Optional[Dict] = None, mode: str = "daily"
) -> str:
    """Render Feishu content"""
    text_content = ""

    if report_data["stats"]:
        text_content += f"📊 **Trending Keywords Stats**\n\n"

    total_count = len(report_data["stats"])

    for i, stat in enumerate(report_data["stats"]):
        word = stat["word"]
        count = stat["count"]

        sequence_display = f"<font color='grey'>[{i + 1}/{total_count}]</font>"

        if count >= 10:
            text_content += f"🔥 {sequence_display} **{word}** : <font color='red'>{count}</font> items\n\n"
        elif count >= 5:
            text_content += f"📈 {sequence_display} **{word}** : <font color='orange'>{count}</font> items\n\n"
        else:
            text_content += f"📌 {sequence_display} **{word}** : {count} items\n\n"

        for j, title_data in enumerate(stat["titles"], 1):
            formatted_title = format_title_for_platform(
                "feishu", title_data, show_source=True
            )
            text_content += f"  {j}. {formatted_title}\n"

            if j < len(stat["titles"]):
                text_content += "\n"

        if i < len(report_data["stats"]) - 1:
            text_content += f"\n{CONFIG['FEISHU_MESSAGE_SEPARATOR']}\n\n"

    if not text_content:
        if mode == "incremental":
            mode_text = "No new matching trending keywords in incremental mode"
        elif mode == "current":
            mode_text = "No matching trending keywords in current ranking mode"
        else:
            mode_text = "No matching trending keywords"
        text_content = f"📭 {mode_text}\n\n"

    if report_data["new_titles"]:
        if text_content and "No matching" not in text_content:
            text_content += f"\n{CONFIG['FEISHU_MESSAGE_SEPARATOR']}\n\n"

        text_content += (
            f"🆕 **New Trending News** (Total {report_data['total_new_count']} items)\n\n"
        )

        for source_data in report_data["new_titles"]:
            text_content += (
                f"**{source_data['source_name']}** ({len(source_data['titles'])} items):\n"
            )

            for j, title_data in enumerate(source_data["titles"], 1):
                title_data_copy = title_data.copy()
                title_data_copy["is_new"] = False
                formatted_title = format_title_for_platform(
                    "feishu", title_data_copy, show_source=False
                )
                text_content += f"  {j}. {formatted_title}\n"

            text_content += "\n"

    if report_data["failed_ids"]:
        if text_content and "No matching" not in text_content:
            text_content += f"\n{CONFIG['FEISHU_MESSAGE_SEPARATOR']}\n\n"

        text_content += "⚠️ **Platforms Failed to Fetch:**\n\n"
        for i, id_value in enumerate(report_data["failed_ids"], 1):
            text_content += f"  • <font color='red'>{id_value}</font>\n"

    now = get_beijing_time()
    text_content += (
        f"\n\n<font color='grey'>Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}</font>"
    )

    if update_info:
        text_content += f"\n<font color='grey'>TrendRadar New Version {update_info['remote_version']} Found, Current {update_info['current_version']}</font>"

    return text_content


def render_dingtalk_content(
    report_data: Dict, update_info: Optional[Dict] = None, mode: str = "daily"
) -> str:
    """Render DingTalk content"""
    text_content = ""

    total_titles = sum(
        len(stat["titles"]) for stat in report_data["stats"] if stat["count"] > 0
    )
    now = get_beijing_time()

    text_content += f"**Total News:** {total_titles}\n\n"
    text_content += f"**Time:** {now.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    text_content += f"**Type:** TrendRadar Analysis Report\n\n"

    text_content += "---\n\n"

    if report_data["stats"]:
        text_content += f"📊 **Trending Keywords Stats**\n\n"

        total_count = len(report_data["stats"])

        for i, stat in enumerate(report_data["stats"]):
            word = stat["word"]
            count = stat["count"]

            sequence_display = f"[{i + 1}/{total_count}]"

            if count >= 10:
                text_content += f"🔥 {sequence_display} **{word}** : **{count}** items\n\n"
            elif count >= 5:
                text_content += f"📈 {sequence_display} **{word}** : **{count}** items\n\n"
            else:
                text_content += f"📌 {sequence_display} **{word}** : {count} items\n\n"

            for j, title_data in enumerate(stat["titles"], 1):
                formatted_title = format_title_for_platform(
                    "dingtalk", title_data, show_source=True
                )
                text_content += f"  {j}. {formatted_title}\n"

                if j < len(stat["titles"]):
                    text_content += "\n"

            if i < len(report_data["stats"]) - 1:
                text_content += f"\n---\n\n"

    if not report_data["stats"]:
        if mode == "incremental":
            mode_text = "No new matching trending keywords in incremental mode"
        elif mode == "current":
            mode_text = "No matching trending keywords in current ranking mode"
        else:
            mode_text = "No matching trending keywords"
        text_content += f"📭 {mode_text}\n\n"

    if report_data["new_titles"]:
        if text_content and "No matching" not in text_content:
            text_content += f"\n---\n\n"

        text_content += (
            f"🆕 **New Trending News** (Total {report_data['total_new_count']} items)\n\n"
        )

        for source_data in report_data["new_titles"]:
            text_content += f"**{source_data['source_name']}** ({len(source_data['titles'])} items):\n\n"

            for j, title_data in enumerate(source_data["titles"], 1):
                title_data_copy = title_data.copy()
                title_data_copy["is_new"] = False
                formatted_title = format_title_for_platform(
                    "dingtalk", title_data_copy, show_source=False
                )
                text_content += f"  {j}. {formatted_title}\n"

            text_content += "\n"

    if report_data["failed_ids"]:
        if text_content and "No matching" not in text_content:
            text_content += f"\n---\n\n"

        text_content += "⚠️ **Platforms Failed to Fetch:**\n\n"
        for i, id_value in enumerate(report_data["failed_ids"], 1):
            text_content += f"  • **{id_value}**\n"

    text_content += f"\n\n> Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}"

    if update_info:
        text_content += f"\n> TrendRadar New Version **{update_info['remote_version']}** Found, Current **{update_info['current_version']}**"

    return text_content


def split_content_into_batches(
    report_data: Dict,
    format_type: str,
    update_info: Optional[Dict] = None,
    max_bytes: int = None,
    mode: str = "daily",
) -> List[str]:
    """Split content into batches, ensuring integrity of keyword group header + at least first news item"""
    if max_bytes is None:
        if format_type == "dingtalk":
            max_bytes = CONFIG.get("DINGTALK_BATCH_SIZE", 20000)
        elif format_type == "feishu":
            max_bytes = CONFIG.get("FEISHU_BATCH_SIZE", 29000)
        elif format_type == "ntfy":
            max_bytes = 3800
        else:
            max_bytes = CONFIG.get("MESSAGE_BATCH_SIZE", 4000)

    batches = []

    total_titles = sum(
        len(stat["titles"]) for stat in report_data["stats"] if stat["count"] > 0
    )
    now = get_beijing_time()

    base_header = ""
    if format_type == "wework":
        base_header = f"**Total News:** {total_titles}\n\n\n\n"
    elif format_type == "telegram":
        base_header = f"Total News: {total_titles}\n\n"
    elif format_type == "ntfy":
        base_header = f"**Total News:** {total_titles}\n\n"
    elif format_type == "feishu":
        base_header = ""
    elif format_type == "dingtalk":
        base_header = f"**Total News:** {total_titles}\n\n"
        base_header += f"**Time:** {now.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        base_header += f"**Type:** TrendRadar Analysis Report\n\n"
        base_header += "---\n\n"

    base_footer = ""
    if format_type == "wework":
        base_footer = f"\n\n\n> Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        if update_info:
            base_footer += f"\n> TrendRadar New Version **{update_info['remote_version']}** Found, Current **{update_info['current_version']}**"
    elif format_type == "telegram":
        base_footer = f"\n\nUpdated: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        if update_info:
            base_footer += f"\nTrendRadar New Version {update_info['remote_version']} Found, Current {update_info['current_version']}"
    elif format_type == "ntfy":
        base_footer = f"\n\n> Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        if update_info:
            base_footer += f"\n> TrendRadar New Version **{update_info['remote_version']}** Found, Current **{update_info['current_version']}**"
    elif format_type == "feishu":
        base_footer = f"\n\n<font color='grey'>Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}</font>"
        if update_info:
            base_footer += f"\n<font color='grey'>TrendRadar New Version {update_info['remote_version']} Found, Current {update_info['current_version']}</font>"
    elif format_type == "dingtalk":
        base_footer = f"\n\n> Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        if update_info:
            base_footer += f"\n> TrendRadar New Version **{update_info['remote_version']}** Found, Current **{update_info['current_version']}**"

    stats_header = ""
    if report_data["stats"]:
        if format_type == "wework":
            stats_header = f"📊 **Trending Keywords Stats**\n\n"
        elif format_type == "telegram":
            stats_header = f"📊 Trending Keywords Stats\n\n"
        elif format_type == "ntfy":
            stats_header = f"📊 **Trending Keywords Stats**\n\n"
        elif format_type == "feishu":
            stats_header = f"📊 **Trending Keywords Stats**\n\n"
        elif format_type == "dingtalk":
            stats_header = f"📊 **Trending Keywords Stats**\n\n"

    current_batch = base_header
    current_batch_has_content = False

    if (
        not report_data["stats"]
        and not report_data["new_titles"]
        and not report_data["failed_ids"]
    ):
        if mode == "incremental":
            mode_text = "No new matching trending keywords in incremental mode"
        elif mode == "current":
            mode_text = "No matching trending keywords in current ranking mode"
        else:
            mode_text = "No matching trending keywords"
        simple_content = f"📭 {mode_text}\n\n"
        final_content = base_header + simple_content + base_footer
        batches.append(final_content)
        return batches

    # Process trending keywords stats
    if report_data["stats"]:
        total_count = len(report_data["stats"])

        # Add stats header
        test_content = current_batch + stats_header
        if (
            len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
            < max_bytes
        ):
            current_batch = test_content
            current_batch_has_content = True
        else:
            if current_batch_has_content:
                batches.append(current_batch + base_footer)
            current_batch = base_header + stats_header
            current_batch_has_content = True

        # Process each keyword group (ensure atomicity of group header + first news item)
        for i, stat in enumerate(report_data["stats"]):
            word = stat["word"]
            count = stat["count"]
            sequence_display = f"[{i + 1}/{total_count}]"

            # Build keyword group header
            word_header = ""
            if format_type == "wework":
                if count >= 10:
                    word_header = (
                        f"🔥 {sequence_display} **{word}** : **{count}** items\n\n"
                    )
                elif count >= 5:
                    word_header = (
                        f"📈 {sequence_display} **{word}** : **{count}** items\n\n"
                    )
                else:
                    word_header = f"📌 {sequence_display} **{word}** : {count} items\n\n"
            elif format_type == "telegram":
                if count >= 10:
                    word_header = f"🔥 {sequence_display} {word} : {count} items\n\n"
                elif count >= 5:
                    word_header = f"📈 {sequence_display} {word} : {count} items\n\n"
                else:
                    word_header = f"📌 {sequence_display} {word} : {count} items\n\n"
            elif format_type == "ntfy":
                if count >= 10:
                    word_header = (
                        f"🔥 {sequence_display} **{word}** : **{count}** items\n\n"
                    )
                elif count >= 5:
                    word_header = (
                        f"📈 {sequence_display} **{word}** : **{count}** items\n\n"
                    )
                else:
                    word_header = f"📌 {sequence_display} **{word}** : {count} items\n\n"
            elif format_type == "feishu":
                if count >= 10:
                    word_header = f"🔥 <font color='grey'>{sequence_display}</font> **{word}** : <font color='red'>{count}</font> items\n\n"
                elif count >= 5:
                    word_header = f"📈 <font color='grey'>{sequence_display}</font> **{word}** : <font color='orange'>{count}</font> items\n\n"
                else:
                    word_header = f"📌 <font color='grey'>{sequence_display}</font> **{word}** : {count} items\n\n"
            elif format_type == "dingtalk":
                if count >= 10:
                    word_header = (
                        f"🔥 {sequence_display} **{word}** : **{count}** items\n\n"
                    )
                elif count >= 5:
                    word_header = (
                        f"📈 {sequence_display} **{word}** : **{count}** items\n\n"
                    )
                else:
                    word_header = f"📌 {sequence_display} **{word}** : {count} items\n\n"

            # Build first news item
            first_news_line = ""
            if stat["titles"]:
                first_title_data = stat["titles"][0]
                if format_type == "wework":
                    formatted_title = format_title_for_platform(
                        "wework", first_title_data, show_source=True
                    )
                elif format_type == "telegram":
                    formatted_title = format_title_for_platform(
                        "telegram", first_title_data, show_source=True
                    )
                elif format_type == "ntfy":
                    formatted_title = format_title_for_platform(
                        "ntfy", first_title_data, show_source=True
                    )
                elif format_type == "feishu":
                    formatted_title = format_title_for_platform(
                        "feishu", first_title_data, show_source=True
                    )
                elif format_type == "dingtalk":
                    formatted_title = format_title_for_platform(
                        "dingtalk", first_title_data, show_source=True
                    )
                else:
                    formatted_title = f"{first_title_data['title']}"

                first_news_line = f"  1. {formatted_title}\n"
                if len(stat["titles"]) > 1:
                    first_news_line += "\n"

            # Atomicity check: keyword header + first news item must be processed together
            word_with_first_news = word_header + first_news_line
            test_content = current_batch + word_with_first_news

            if (
                len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
                >= max_bytes
            ):
                # Current batch cannot fit, start new batch
                if current_batch_has_content:
                    batches.append(current_batch + base_footer)
                current_batch = base_header + stats_header + word_with_first_news
                current_batch_has_content = True
                start_index = 1
            else:
                current_batch = test_content
                current_batch_has_content = True
                start_index = 1

            # Process remaining news items
            for j in range(start_index, len(stat["titles"])):
                title_data = stat["titles"][j]
                if format_type == "wework":
                    formatted_title = format_title_for_platform(
                        "wework", title_data, show_source=True
                    )
                elif format_type == "telegram":
                    formatted_title = format_title_for_platform(
                        "telegram", title_data, show_source=True
                    )
                elif format_type == "ntfy":
                    formatted_title = format_title_for_platform(
                        "ntfy", title_data, show_source=True
                    )
                elif format_type == "feishu":
                    formatted_title = format_title_for_platform(
                        "feishu", title_data, show_source=True
                    )
                elif format_type == "dingtalk":
                    formatted_title = format_title_for_platform(
                        "dingtalk", title_data, show_source=True
                    )
                else:
                    formatted_title = f"{title_data['title']}"

                news_line = f"  {j + 1}. {formatted_title}\n"
                if j < len(stat["titles"]) - 1:
                    news_line += "\n"

                test_content = current_batch + news_line
                if (
                    len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
                    >= max_bytes
                ):
                    if current_batch_has_content:
                        batches.append(current_batch + base_footer)
                    current_batch = base_header + stats_header + word_header + news_line
                    current_batch_has_content = True
                else:
                    current_batch = test_content
                    current_batch_has_content = True

            # Separator between keyword groups
            if i < len(report_data["stats"]) - 1:
                separator = ""
                if format_type == "wework":
                    separator = f"\n\n\n\n"
                elif format_type == "telegram":
                    separator = f"\n\n"
                elif format_type == "ntfy":
                    separator = f"\n\n"
                elif format_type == "feishu":
                    separator = f"\n{CONFIG['FEISHU_MESSAGE_SEPARATOR']}\n\n"
                elif format_type == "dingtalk":
                    separator = f"\n---\n\n"

                test_content = current_batch + separator
                if (
                    len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
                    < max_bytes
                ):
                    current_batch = test_content

    # Process new news (ensure atomicity of source header + first news item)
    if report_data["new_titles"]:
        new_header = ""
        if format_type == "wework":
            new_header = f"\n\n\n\n🆕 **New Trending News** (Total {report_data['total_new_count']} items)\n\n"
        elif format_type == "telegram":
            new_header = (
                f"\n\n🆕 New Trending News (Total {report_data['total_new_count']} items)\n\n"
            )
        elif format_type == "ntfy":
            new_header = f"\n\n🆕 **New Trending News** (Total {report_data['total_new_count']} items)\n\n"
        elif format_type == "feishu":
            new_header = f"\n{CONFIG['FEISHU_MESSAGE_SEPARATOR']}\n\n🆕 **New Trending News** (Total {report_data['total_new_count']} items)\n\n"
        elif format_type == "dingtalk":
            new_header = f"\n---\n\n🆕 **New Trending News** (Total {report_data['total_new_count']} items)\n\n"

        test_content = current_batch + new_header
        if (
            len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
            >= max_bytes
        ):
            if current_batch_has_content:
                batches.append(current_batch + base_footer)
            current_batch = base_header + new_header
            current_batch_has_content = True
        else:
            current_batch = test_content
            current_batch_has_content = True

        # Process each new news source
        for source_data in report_data["new_titles"]:
            source_header = ""
            if format_type == "wework":
                source_header = f"**{source_data['source_name']}** ({len(source_data['titles'])} items):\n\n"
            elif format_type == "telegram":
                source_header = f"{source_data['source_name']} ({len(source_data['titles'])} items):\n\n"
            elif format_type == "ntfy":
                source_header = f"**{source_data['source_name']}** ({len(source_data['titles'])} items):\n\n"
            elif format_type == "feishu":
                source_header = f"**{source_data['source_name']}** ({len(source_data['titles'])} items):\n\n"
            elif format_type == "dingtalk":
                source_header = f"**{source_data['source_name']}** ({len(source_data['titles'])} items):\n\n"

            # Build first new news item
            first_news_line = ""
            if source_data["titles"]:
                first_title_data = source_data["titles"][0]
                title_data_copy = first_title_data.copy()
                title_data_copy["is_new"] = False

                if format_type == "wework":
                    formatted_title = format_title_for_platform(
                        "wework", title_data_copy, show_source=False
                    )
                elif format_type == "telegram":
                    formatted_title = format_title_for_platform(
                        "telegram", title_data_copy, show_source=False
                    )
                elif format_type == "feishu":
                    formatted_title = format_title_for_platform(
                        "feishu", title_data_copy, show_source=False
                    )
                elif format_type == "dingtalk":
                    formatted_title = format_title_for_platform(
                        "dingtalk", title_data_copy, show_source=False
                    )
                else:
                    formatted_title = f"{title_data_copy['title']}"

                first_news_line = f"  1. {formatted_title}\n"

            # Atomicity check: source header + first news item
            source_with_first_news = source_header + first_news_line
            test_content = current_batch + source_with_first_news

            if (
                len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
                >= max_bytes
            ):
                if current_batch_has_content:
                    batches.append(current_batch + base_footer)
                current_batch = base_header + new_header + source_with_first_news
                current_batch_has_content = True
                start_index = 1
            else:
                current_batch = test_content
                current_batch_has_content = True
                start_index = 1

            # Process remaining new news items
            for j in range(start_index, len(source_data["titles"])):
                title_data = source_data["titles"][j]
                title_data_copy = title_data.copy()
                title_data_copy["is_new"] = False

                if format_type == "wework":
                    formatted_title = format_title_for_platform(
                        "wework", title_data_copy, show_source=False
                    )
                elif format_type == "telegram":
                    formatted_title = format_title_for_platform(
                        "telegram", title_data_copy, show_source=False
                    )
                elif format_type == "feishu":
                    formatted_title = format_title_for_platform(
                        "feishu", title_data_copy, show_source=False
                    )
                elif format_type == "dingtalk":
                    formatted_title = format_title_for_platform(
                        "dingtalk", title_data_copy, show_source=False
                    )
                else:
                    formatted_title = f"{title_data_copy['title']}"

                news_line = f"  {j + 1}. {formatted_title}\n"

                test_content = current_batch + news_line
                if (
                    len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
                    >= max_bytes
                ):
                    if current_batch_has_content:
                        batches.append(current_batch + base_footer)
                    current_batch = base_header + new_header + source_header + news_line
                    current_batch_has_content = True
                else:
                    current_batch = test_content
                    current_batch_has_content = True

            current_batch += "\n"

    if report_data["failed_ids"]:
        failed_header = ""
        if format_type == "wework":
            failed_header = f"\n\n\n\n⚠️ **Platforms Failed to Fetch:**\n\n"
        elif format_type == "telegram":
            failed_header = f"\n\n⚠️ Platforms Failed to Fetch:\n\n"
        elif format_type == "ntfy":
            failed_header = f"\n\n⚠️ **Platforms Failed to Fetch:**\n\n"
        elif format_type == "feishu":
            failed_header = f"\n{CONFIG['FEISHU_MESSAGE_SEPARATOR']}\n\n⚠️ **Platforms Failed to Fetch:**\n\n"
        elif format_type == "dingtalk":
            failed_header = f"\n---\n\n⚠️ **Platforms Failed to Fetch:**\n\n"

        test_content = current_batch + failed_header
        if (
            len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
            >= max_bytes
        ):
            if current_batch_has_content:
                batches.append(current_batch + base_footer)
            current_batch = base_header + failed_header
            current_batch_has_content = True
        else:
            current_batch = test_content
            current_batch_has_content = True

        for i, id_value in enumerate(report_data["failed_ids"], 1):
            if format_type == "feishu":
                failed_line = f"  • <font color='red'>{id_value}</font>\n"
            elif format_type == "dingtalk":
                failed_line = f"  • **{id_value}**\n"
            else:
                failed_line = f"  • {id_value}\n"

            test_content = current_batch + failed_line
            if (
                len(test_content.encode("utf-8")) + len(base_footer.encode("utf-8"))
                >= max_bytes
            ):
                if current_batch_has_content:
                    batches.append(current_batch + base_footer)
                current_batch = base_header + failed_header + failed_line
                current_batch_has_content = True
            else:
                current_batch = test_content
                current_batch_has_content = True

    # Finish last batch
    if current_batch_has_content:
        batches.append(current_batch + base_footer)

    return batches


def send_to_notifications(
    stats: List[Dict],
    failed_ids: Optional[List] = None,
    report_type: str = "Daily Summary",
    new_titles: Optional[Dict] = None,
    id_to_name: Optional[Dict] = None,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
    html_file_path: Optional[str] = None,
) -> Dict[str, bool]:
    """Send data to multiple notification platforms"""
    results = {}
    any_success = False

    # Push window check
    if CONFIG["PUSH_WINDOW"].get("ENABLED", False):
        push_manager = PushRecordManager()
        time_range_start = CONFIG["PUSH_WINDOW"]["TIME_RANGE"]["START"]
        time_range_end = CONFIG["PUSH_WINDOW"]["TIME_RANGE"]["END"]

        if not push_manager.is_in_time_range(time_range_start, time_range_end):
            now = get_beijing_time()
            print(
                f"Push window control: Current time {now.strftime('%H:%M')} not within push window {time_range_start}-{time_range_end}, skipping push."
            )
            return results

        if CONFIG["PUSH_WINDOW"]["ONCE_PER_DAY"]:
            if push_manager.has_pushed_today():
                print(f"Push window control: Already pushed today, skipping this push.")
                return results
            else:
                print(f"Push window control: First push today.")

    report_data = prepare_report_data(stats, failed_ids, new_titles, id_to_name, mode)

    # Send to Feishu
    if CONFIG["FEISHU_WEBHOOK_URL"]:
        success = send_to_feishu(
            CONFIG["FEISHU_WEBHOOK_URL"],
            report_data,
            report_type,
            update_info,
            proxy_url,
            mode=mode,
        )
        if success:
            any_success = True
        results["feishu"] = success

    # Send to DingTalk
    if CONFIG["DINGTALK_WEBHOOK_URL"]:
        success = send_to_dingtalk(
            CONFIG["DINGTALK_WEBHOOK_URL"],
            report_data,
            report_type,
            update_info,
            proxy_url,
            mode=mode,
        )
        if success:
            any_success = True
        results["dingtalk"] = success

    # Send to WeWork
    if CONFIG["WEWORK_WEBHOOK_URL"]:
        success = send_to_wework(
            CONFIG["WEWORK_WEBHOOK_URL"],
            report_data,
            report_type,
            update_info,
            proxy_url,
            mode=mode,
        )
        if success:
            any_success = True
        results["wework"] = success

    # Send to Telegram
    if CONFIG["TELEGRAM_BOT_TOKEN"] and CONFIG["TELEGRAM_CHAT_ID"]:
        success = send_to_telegram(
            CONFIG["TELEGRAM_BOT_TOKEN"],
            CONFIG["TELEGRAM_CHAT_ID"],
            report_data,
            report_type,
            update_info,
            proxy_url,
            mode=mode,
        )
        if success:
            any_success = True
        results["telegram"] = success

    # Send to ntfy
    if CONFIG["NTFY_SERVER_URL"] and CONFIG["NTFY_TOPIC"]:
        success = send_to_ntfy(
            CONFIG["NTFY_SERVER_URL"],
            CONFIG["NTFY_TOPIC"],
            CONFIG["NTFY_TOKEN"],
            report_data,
            report_type,
            update_info,
            proxy_url,
            mode=mode,
        )
        if success:
            any_success = True
        results["ntfy"] = success

    # Send to Email
    if (
        CONFIG["EMAIL_FROM"]
        and CONFIG["EMAIL_PASSWORD"]
        and CONFIG["EMAIL_TO"]
        and html_file_path
    ):
        success = send_to_email(
            CONFIG["EMAIL_FROM"],
            CONFIG["EMAIL_PASSWORD"],
            CONFIG["EMAIL_TO"],
            report_type,
            html_file_path,
            CONFIG.get("EMAIL_SMTP_SERVER"),
            CONFIG.get("EMAIL_SMTP_PORT"),
        )
        if success:
            any_success = True
        results["email"] = success

    if not any_success:
        print("No notification channels configured or all failed, skipping.")

    # If any notification sent successfully and daily limit enabled, record push
    if (
        CONFIG["PUSH_WINDOW"]["ENABLED"]
        and CONFIG["PUSH_WINDOW"]["ONCE_PER_DAY"]
        and any_success
    ):
        push_manager = PushRecordManager()
        push_manager.record_push(report_type)

    # Daily limit check (if enabled)
    push_manager = PushRecordManager()
    if (
        CONFIG["PUSH_WINDOW"]["ENABLED"]
        and CONFIG["PUSH_WINDOW"]["ONCE_PER_DAY"]
        and push_manager.has_pushed_today(report_type)
    ):
        print(f"Push limit control: Already pushed today, skipping this push.")
        return results
    
    if CONFIG["PUSH_WINDOW"]["ENABLED"] and CONFIG["PUSH_WINDOW"]["ONCE_PER_DAY"]:
        print(f"Push limit control: First push today.")

    return results


def send_to_feishu(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
) -> bool:
    """Send to Feishu (supports batch sending)"""
    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # Get batch content, using Feishu specific batch size
    batches = split_content_into_batches(
        report_data,
        "feishu",
        update_info,
        max_bytes=CONFIG.get("FEISHU_BATCH_SIZE", 29000),
        mode=mode,
    )

    print(f"Feishu message split into {len(batches)} batches [{report_type}]")

    # Send in batches
    for i, batch_content in enumerate(batches, 1):
        batch_size = len(batch_content.encode("utf-8"))
        print(
            f"Sending Feishu batch {i}/{len(batches)}, size: {batch_size} bytes [{report_type}]"
        )

        # Add batch identifier
        if len(batches) > 1:
            batch_header = f"**[Batch {i}/{len(batches)}]**\n\n"
            # Insert batch identifier at appropriate position (after statistics title)
            if "📊 **Trending Keywords Stats**" in batch_content:
                batch_content = batch_content.replace(
                    "📊 **Trending Keywords Stats**\n\n", f"📊 **Trending Keywords Stats** {batch_header}"
                )
            else:
                # If no statistics title, add directly at the beginning
                batch_content = batch_header + batch_content

        total_titles = sum(
            len(stat["titles"]) for stat in report_data["stats"] if stat["count"] > 0
        )
        now = get_beijing_time()

        payload = {
            "msg_type": "text",
            "content": {
                "total_titles": total_titles,
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                "report_type": report_type,
                "text": batch_content,
            },
        }

        try:
            response = requests.post(
                webhook_url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                # Check Feishu response status
                if result.get("StatusCode") == 0 or result.get("code") == 0:
                    print(f"Feishu batch {i}/{len(batches)} sent successfully [{report_type}]")
                    # Interval between batches
                    if i < len(batches):
                        time.sleep(CONFIG["BATCH_SEND_INTERVAL"])
                else:
                    error_msg = result.get("msg") or result.get("StatusMessage", "Unknown Error")
                    print(
                        f"Feishu batch {i}/{len(batches)} failed [{report_type}], error: {error_msg}"
                    )
                    return False
            else:
                print(
                    f"Feishu batch {i}/{len(batches)} failed [{report_type}], status code: {response.status_code}"
                )
                return False
        except Exception as e:
            print(f"Feishu batch {i}/{len(batches)} error [{report_type}]: {e}")
            return False

    print(f"Feishu all {len(batches)} batches sent [{report_type}]")
    return True


def send_to_dingtalk(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
) -> bool:
    """Send to DingTalk (supports batch sending)"""
    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # Get batch content, using DingTalk specific batch size
    batches = split_content_into_batches(
        report_data,
        "dingtalk",
        update_info,
        max_bytes=CONFIG.get("DINGTALK_BATCH_SIZE", 20000),
        mode=mode,
    )

    print(f"DingTalk message split into {len(batches)} batches [{report_type}]")

    # Send in batches
    for i, batch_content in enumerate(batches, 1):
        batch_size = len(batch_content.encode("utf-8"))
        print(
            f"Sending DingTalk batch {i}/{len(batches)}, size: {batch_size} bytes [{report_type}]"
        )

        # Add batch identifier
        if len(batches) > 1:
            batch_header = f"**[Batch {i}/{len(batches)}]**\n\n"
            # Insert batch identifier at appropriate position (after title)
            if "📊 **Trending Keywords Stats**" in batch_content:
                batch_content = batch_content.replace(
                    "📊 **Trending Keywords Stats**\n\n", f"📊 **Trending Keywords Stats** {batch_header}\n\n"
                )
            else:
                # If no statistics title, add directly at the beginning
                batch_content = batch_header + batch_content

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"TrendRadar Analysis Report - {report_type}",
                "text": batch_content,
            },
        }

        try:
            response = requests.post(
                webhook_url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    print(f"DingTalk batch {i}/{len(batches)} sent successfully [{report_type}]")
                    # Interval between batches
                    if i < len(batches):
                        time.sleep(CONFIG["BATCH_SEND_INTERVAL"])
                else:
                    print(
                        f"DingTalk batch {i}/{len(batches)} failed [{report_type}], error: {result.get('errmsg')}"
                    )
                    return False
            else:
                print(
                    f"DingTalk batch {i}/{len(batches)} failed [{report_type}], status code: {response.status_code}"
                )
                return False
        except Exception as e:
            print(f"DingTalk batch {i}/{len(batches)} error [{report_type}]: {e}")
            return False

    print(f"DingTalk all {len(batches)} batches sent [{report_type}]")
    return True


def send_to_wework(
    webhook_url: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
) -> bool:
    """Send to WeWork (supports batch sending)"""
    headers = {"Content-Type": "application/json"}
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # Get batch content
    batches = split_content_into_batches(report_data, "wework", update_info, mode=mode)

    print(f"WeWork message split into {len(batches)} batches [{report_type}]")

    # Send in batches
    for i, batch_content in enumerate(batches, 1):
        batch_size = len(batch_content.encode("utf-8"))
        print(
            f"Sending WeWork batch {i}/{len(batches)}, size: {batch_size} bytes [{report_type}]"
        )

        # Add batch identifier
        if len(batches) > 1:
            batch_header = f"**[Batch {i}/{len(batches)}]**\n\n"
            batch_content = batch_header + batch_content

        payload = {"msgtype": "markdown", "markdown": {"content": batch_content}}

        try:
            response = requests.post(
                webhook_url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("errcode") == 0:
                    print(f"WeWork batch {i}/{len(batches)} sent successfully [{report_type}]")
                    # Interval between batches
                    if i < len(batches):
                        time.sleep(CONFIG["BATCH_SEND_INTERVAL"])
                else:
                    print(
                        f"WeWork batch {i}/{len(batches)} failed [{report_type}], error: {result.get('errmsg')}"
                    )
                    return False
            else:
                print(
                    f"WeWork batch {i}/{len(batches)} failed [{report_type}], status code: {response.status_code}"
                )
                return False
        except Exception as e:
            print(f"WeWork batch {i}/{len(batches)} error [{report_type}]: {e}")
            return False

    print(f"WeWork all {len(batches)} batches sent [{report_type}]")
    return True


def send_to_telegram(
    bot_token: str,
    chat_id: str,
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
) -> bool:
    """Send to Telegram (supports batch sending)"""
    headers = {"Content-Type": "application/json"}
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # Get batch content
    batches = split_content_into_batches(
        report_data, "telegram", update_info, mode=mode
    )

    print(f"Telegram message split into {len(batches)} batches [{report_type}]")

    # Send in batches
    for i, batch_content in enumerate(batches, 1):
        batch_size = len(batch_content.encode("utf-8"))
        print(
            f"Sending Telegram batch {i}/{len(batches)}, size: {batch_size} bytes [{report_type}]"
        )

        # Add batch identifier
        if len(batches) > 1:
            batch_header = f"<b>[Batch {i}/{len(batches)}]</b>\n\n"
            batch_content = batch_header + batch_content

        payload = {
            "chat_id": chat_id,
            "text": batch_content,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            response = requests.post(
                url, headers=headers, json=payload, proxies=proxies, timeout=30
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("ok"):
                    print(f"Telegram batch {i}/{len(batches)} sent successfully [{report_type}]")
                    # Interval between batches
                    if i < len(batches):
                        time.sleep(CONFIG["BATCH_SEND_INTERVAL"])
                else:
                    print(
                        f"Telegram batch {i}/{len(batches)} failed [{report_type}], error: {result.get('description')}"
                    )
                    return False
            else:
                print(
                    f"Telegram batch {i}/{len(batches)} failed [{report_type}], status code: {response.status_code}"
                )
                return False
        except Exception as e:
            print(f"Telegram batch {i}/{len(batches)} error [{report_type}]: {e}")
            return False

    print(f"Telegram all {len(batches)} batches sent [{report_type}]")
    return True


def send_to_email(
    from_email: str,
    password: str,
    to_email: str,
    report_type: str,
    html_file_path: str,
    custom_smtp_server: Optional[str] = None,
    custom_smtp_port: Optional[int] = None,
) -> bool:
    """Send email notification"""
    try:
        # Check for email-optimized version first
        email_html_path = html_file_path.replace(".html", "_email.html")
        final_html_path = html_file_path
        
        if Path(email_html_path).exists():
            print(f"Using email-optimized HTML file: {email_html_path}")
            final_html_path = email_html_path
        elif not Path(html_file_path).exists():
            print(f"Error: HTML file not found or not provided: {html_file_path}")
            return False
        else:
            print(f"Using standard HTML file: {html_file_path}")

        with open(final_html_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        domain = from_email.split("@")[-1].lower()

        if custom_smtp_server and custom_smtp_port:
            # Use custom SMTP config
            smtp_server = custom_smtp_server
            smtp_port = int(custom_smtp_port)
            # Determine encryption based on port: 465=SSL, 587=TLS
            if smtp_port == 465:
                use_tls = False  # SSL mode (SMTP_SSL)
            elif smtp_port == 587:
                use_tls = True   # TLS mode (STARTTLS)
            else:
                # Prefer TLS for other ports (more secure, widely supported)
                use_tls = True
        elif domain in SMTP_CONFIGS:
            # Use preset config
            config = SMTP_CONFIGS[domain]
            smtp_server = config["server"]
            smtp_port = config["port"]
            use_tls = config["encryption"] == "TLS"
        else:
            print(f"Unrecognized email provider: {domain}, using generic SMTP config")
            smtp_server = f"smtp.{domain}"
            smtp_port = 587
            use_tls = True

        msg = MIMEMultipart("alternative")

        # Set From header strictly according to RFC standards
        sender_name = "TrendRadar"
        msg["From"] = formataddr((sender_name, from_email))

        # Set recipients
        recipients = [addr.strip() for addr in to_email.split(",")]
        if len(recipients) == 1:
            msg["To"] = recipients[0]
        else:
            msg["To"] = ", ".join(recipients)

        # Set email subject
        now = get_beijing_time()
        now = get_beijing_time()
        subject = f"TrendRadar Trending Analysis Report - {report_type} - {now.strftime('%m-%d %H:%M')}"
        msg["Subject"] = Header(subject, "utf-8")

        # Set other standard headers
        msg["MIME-Version"] = "1.0"
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid()

        # Add plain text part (as fallback)
        text_content = f"""
TrendRadar Trending Analysis Report
========================
Report Type: {report_type}
Generated Time: {now.strftime('%Y-%m-%d %H:%M:%S')}

Please use an HTML-supported email client to view the full report.
        """
        text_part = MIMEText(text_content, "plain", "utf-8")
        msg.attach(text_part)

        html_part = MIMEText(html_content, "html", "utf-8")
        msg.attach(html_part)

        print(f"Sending email to {to_email}...")
        print(f"SMTP Server: {smtp_server}:{smtp_port}")
        print(f"Sender: {from_email}")

        try:
            if use_tls:
                # TLS mode
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)  # Set to 1 for detailed debug info
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                # SSL mode
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)
                server.ehlo()

            # Login
            server.login(from_email, password)

            # Send email
            server.send_message(msg)
            server.quit()

            print(f"Email sent successfully [{report_type}] -> {to_email}")
            return True

        except smtplib.SMTPServerDisconnected:
            print(f"Email failed: Server disconnected unexpectedly, please check network or retry later")
            return False

    except smtplib.SMTPAuthenticationError as e:
        print(f"Email failed: Authentication error, please check email/password/app password")
        print(f"Error details: {str(e)}")
        return False
    except smtplib.SMTPRecipientsRefused as e:
        print(f"Email failed: Recipient refused {e}")
        return False
    except smtplib.SMTPSenderRefused as e:
        print(f"Email failed: Sender refused {e}")
        return False
    except smtplib.SMTPDataError as e:
        print(f"Email failed: Data error {e}")
        return False
    except smtplib.SMTPConnectError as e:
        print(f"Email failed: Connection error to {smtp_server}:{smtp_port}")
        print(f"Error details: {str(e)}")
        return False
    except Exception as e:
        print(f"Email failed [{report_type}]: {e}")
        import traceback

        traceback.print_exc()
        return False


def send_to_ntfy(
    server_url: str,
    topic: str,
    token: Optional[str],
    report_data: Dict,
    report_type: str,
    update_info: Optional[Dict] = None,
    proxy_url: Optional[str] = None,
    mode: str = "daily",
) -> bool:
    """Send to ntfy (supports batch sending, strictly follows size limit)"""
    # Avoid HTTP header encoding issues
    report_type_en_map = {
        "当日汇总": "Daily Summary",
        "当前榜单汇总": "Current Ranking",
        "增量更新": "Incremental Update",
        "实时增量": "Realtime Incremental", 
        "实时当前榜单": "Realtime Current Ranking",  
    }
    report_type_en = report_type_en_map.get(report_type, "News Report") 

    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "Markdown": "yes",
        "Title": report_type_en,
        "Priority": "default",
        "Tags": "news",
    }

    if token:
        headers["Authorization"] = f"Bearer {token}"
    
    # Construct full URL, ensuring correct format
    base_url = server_url.rstrip("/")
    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"
    url = f"{base_url}/{topic}"

    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # Get batch content, strictly following ntfy's size limit
    batches = split_content_into_batches(
        report_data, "ntfy", update_info, max_bytes=3800, mode=mode
    )

    total_batches = len(batches)
    print(f"ntfy message split into {total_batches} batches [{report_type}]")

    # Reverse batch order so they appear correctly in ntfy client
    # ntfy shows newest on top, so we push the last batch first
    reversed_batches = list(reversed(batches))
    
    print(f"ntfy pushing in reverse order (last batch first) for correct client display")

    # Send batches (reversed)
    success_count = 0
    for idx, batch_content in enumerate(reversed_batches, 1):
        # Calculate correct batch number (user perspective)
        actual_batch_num = total_batches - idx + 1
        
        batch_size = len(batch_content.encode("utf-8"))
        print(
            f"Sending ntfy batch {actual_batch_num}/{total_batches} (push order: {idx}/{total_batches}), size: {batch_size} bytes [{report_type}]"
        )

        # Check size
        if batch_size > 4096:
            print(f"Warning: ntfy batch {actual_batch_num} too large ({batch_size} bytes), might be rejected")

        # Add batch header (using correct batch number)
        current_headers = headers.copy()
        if total_batches > 1:
            batch_header = f"**[Batch {actual_batch_num}/{total_batches}]**\n\n"
            batch_content = batch_header + batch_content
            current_headers["Title"] = (
                f"{report_type_en} ({actual_batch_num}/{total_batches})"
            )

        try:
            response = requests.post(
                url,
                headers=current_headers,
                data=batch_content.encode("utf-8"),
                proxies=proxies,
                timeout=30,
            )

            if response.status_code == 200:
                print(f"ntfy batch {actual_batch_num}/{total_batches} sent successfully [{report_type}]")
                success_count += 1
                if idx < total_batches:
                    # Public server suggestion 2-3s, self-hosted can be shorter
                    interval = 2 if "ntfy.sh" in server_url else 1
                    time.sleep(interval)
            elif response.status_code == 429:
                print(
                    f"ntfy batch {actual_batch_num}/{total_batches} rate limited [{report_type}], retrying..."
                )
                time.sleep(10)  # Wait 10s
                # Retry once
                retry_response = requests.post(
                    url,
                    headers=current_headers,
                    data=batch_content.encode("utf-8"),
                    proxies=proxies,
                    timeout=30,
                )
                if retry_response.status_code == 200:
                    print(f"ntfy batch {actual_batch_num}/{total_batches} retry successful [{report_type}]")
                    success_count += 1
                else:
                    print(
                        f"ntfy batch {actual_batch_num}/{total_batches} retry failed, status: {retry_response.status_code}"
                    )
            elif response.status_code == 413:
                print(
                    f"ntfy batch {actual_batch_num}/{total_batches} rejected (too large) [{report_type}], size: {batch_size} bytes"
                )
            else:
                print(
                    f"ntfy batch {actual_batch_num}/{total_batches} failed [{report_type}], status: {response.status_code}"
                )
                try:
                    print(f"Error details: {response.text}")
                except:
                    pass

        except requests.exceptions.ConnectTimeout:
            print(f"ntfy batch {actual_batch_num}/{total_batches} connection timeout [{report_type}]")
        except requests.exceptions.ReadTimeout:
            print(f"ntfy batch {actual_batch_num}/{total_batches} read timeout [{report_type}]")
        except requests.exceptions.ConnectionError as e:
            print(f"ntfy batch {actual_batch_num}/{total_batches} connection error [{report_type}]: {e}")
        except Exception as e:
            print(f"ntfy batch {actual_batch_num}/{total_batches} exception [{report_type}]: {e}")

    # Check overall success
    if success_count == total_batches:
        print(f"ntfy all {total_batches} batches sent [{report_type}]")
        return True
    elif success_count > 0:
        print(f"ntfy partial success: {success_count}/{total_batches} batches [{report_type}]")
        return True  # Partial success is still success
    else:
        print(f"ntfy failed completely [{report_type}]")
        return False


# === Main Analyzer ===
class NewsAnalyzer:
    """News Analyzer"""

    # Mode strategy definitions
    MODE_STRATEGIES = {
        "incremental": {
            "mode_name": "Incremental Mode",
            "description": "Incremental mode (real-time push for new matching news + scheduled summary)",
            "realtime_report_type": "Real-time New News",
            "summary_report_type": "Daily Summary",
            "should_send_realtime": True,
            "should_generate_summary": True,
            "summary_mode": "daily",
        },
        "current": {
            "mode_name": "Current Ranking Mode",
            "description": "Current ranking mode (current ranking matching news + new news section + scheduled push)",
            "realtime_report_type": "Real-time Current Ranking",
            "summary_report_type": "Current Ranking Summary",
            "should_send_realtime": True,
            "should_generate_summary": True,
            "summary_mode": "current",
        },
        "daily": {
            "mode_name": "Daily Summary Mode",
            "description": "Daily summary mode (all matching news + new news section + scheduled push)",
            "realtime_report_type": "",
            "summary_report_type": "Daily Summary",
            "should_send_realtime": False,
            "should_generate_summary": True,
            "summary_mode": "daily",
        },
    }

    def __init__(self):
        self.request_interval = CONFIG["REQUEST_INTERVAL"]
        self.report_mode = CONFIG["REPORT_MODE"]
        self.rank_threshold = CONFIG["RANK_THRESHOLD"]
        self.is_github_actions = os.environ.get("GITHUB_ACTIONS") == "true"
        self.is_docker_container = self._detect_docker_environment()
        self.update_info = None
        self.proxy_url = None
        self._setup_proxy()
        self.data_fetcher = DataFetcher(self.proxy_url)

        if self.is_github_actions:
            self._check_version()

    def _detect_docker_environment(self) -> bool:
        """Detect if running in a Docker container"""
        try:
            if os.environ.get("DOCKER_CONTAINER") == "true":
                return True

            if os.path.exists("/.dockerenv"):
                return True

            return False
        except Exception:
            return False

    def _should_open_browser(self) -> bool:
        """Determine if the browser should be opened"""
        return not self.is_github_actions and not self.is_docker_container

    def _setup_proxy(self) -> None:
        """Set up proxy configuration"""
        if not self.is_github_actions and CONFIG["USE_PROXY"]:
            self.proxy_url = CONFIG["DEFAULT_PROXY"]
            print("Local environment, using proxy")
            print("Local environment, using proxy")
        elif not self.is_github_actions and not CONFIG["USE_PROXY"]:
            print("Local environment, proxy not enabled")
        else:
            print("GitHub Actions environment, not using proxy")

    def _check_version(self) -> None:
        """Check for version updates"""
        if not CONFIG["SHOW_VERSION_UPDATE"]:
            return

        try:
            update_checker = UpdateChecker(CONFIG)
            has_update, remote_version, changelog = update_checker.check_update()
            if has_update:
                print(f"New version found: {remote_version} (Current: {VERSION})")
                self.update_info = {
                    "has_update": True,
                    "remote_version": remote_version,
                    "changelog": changelog,
                    "current_version": VERSION,
                }
            else:
                print("Version check complete, you are on the latest version")
        except Exception as e:
            print(f"Version check failed: {e}")

    def _get_mode_strategy(self) -> Dict:
        """Get strategy configuration for the current mode"""
        return self.MODE_STRATEGIES.get(self.report_mode, self.MODE_STRATEGIES["daily"])

    def _has_notification_configured(self) -> bool:
        """Check if any notification channel is configured"""
        return any(
            [
                CONFIG["FEISHU_WEBHOOK_URL"],
                CONFIG["DINGTALK_WEBHOOK_URL"],
                CONFIG["WEWORK_WEBHOOK_URL"],
                (CONFIG["TELEGRAM_BOT_TOKEN"] and CONFIG["TELEGRAM_CHAT_ID"]),
                (
                    CONFIG["EMAIL_FROM"]
                    and CONFIG["EMAIL_PASSWORD"]
                    and CONFIG["EMAIL_TO"]
                ),
                (CONFIG["NTFY_SERVER_URL"] and CONFIG["NTFY_TOPIC"]),
            ]
        )

    def _has_valid_content(
        self, stats: List[Dict], new_titles: Optional[Dict] = None
    ) -> bool:
        """Check if there is valid news content"""
        if self.report_mode in ["incremental", "current"]:
            # In incremental and current modes, if stats has content, it means there are matching news
            return any(stat["count"] > 0 for stat in stats)
        else:
            # In daily summary mode, check for matching frequency words or new news
            has_matched_news = any(stat["count"] > 0 for stat in stats)
            has_new_news = bool(
                new_titles and any(len(titles) > 0 for titles in new_titles.values())
            )
            return has_matched_news or has_new_news

    def _load_analysis_data(
        self,
    ) -> Optional[Tuple[Dict, Dict, Dict, Dict, List, List]]:
        """Unified data loading and preprocessing, filtering historical data using the current monitoring platform list"""
        try:
            # Get current configured platform ID list
            current_platform_ids = []
            for platform in CONFIG["PLATFORMS"]:
                current_platform_ids.append(platform["id"])

            print(f"Current monitoring platforms: {current_platform_ids}")

            all_results, id_to_name, title_info = read_all_today_titles(
                current_platform_ids
            )

            if not all_results:
                print("No data found for today")
                return None

            total_titles = sum(len(titles) for titles in all_results.values())
            print(f"Loaded {total_titles} titles (filtered by current monitoring platforms)")

            new_titles = detect_latest_new_titles(current_platform_ids)
            word_groups, filter_words = load_frequency_words()

            return (
                all_results,
                id_to_name,
                title_info,
                new_titles,
                word_groups,
                filter_words,
            )
        except Exception as e:
            print(f"Data loading failed: {e}")
            return None

    def _prepare_current_title_info(self, results: Dict, time_info: str) -> Dict:
        """Build title information from current crawl results"""
        title_info = {}
        for source_id, titles_data in results.items():
            title_info[source_id] = {}
            for title, title_data in titles_data.items():
                ranks = title_data.get("ranks", [])
                url = title_data.get("url", "")
                mobile_url = title_data.get("mobileUrl", "")

                title_info[source_id][title] = {
                    "first_time": time_info,
                    "last_time": time_info,
                    "count": 1,
                    "ranks": ranks,
                    "url": url,
                    "mobileUrl": mobile_url,
                }
        return title_info

    def _run_analysis_pipeline(
        self,
        data_source: Dict,
        mode: str,
        title_info: Dict,
        new_titles: Dict,
        word_groups: List[Dict],
        filter_words: List[str],
        id_to_name: Dict,
        failed_ids: Optional[List] = None,
        is_daily_summary: bool = False,
    ) -> Tuple[List[Dict], str]:
        """Unified analysis pipeline: Data Processing -> Statistics Calculation -> HTML Generation"""

        # Statistics Calculation
        stats, total_titles = count_word_frequency(
            data_source,
            word_groups,
            filter_words,
            id_to_name,
            title_info,
            self.rank_threshold,
            new_titles,
            mode=mode,
        )

        # HTML Generation
        html_file = generate_html_report(
            stats,
            total_titles,
            failed_ids=failed_ids,
            new_titles=new_titles,
            id_to_name=id_to_name,
            mode=mode,
            is_daily_summary=is_daily_summary,
            update_info=self.update_info if CONFIG["SHOW_VERSION_UPDATE"] else None,
        )

        return stats, html_file

    def _send_notification_if_needed(
        self,
        stats: List[Dict],
        report_type: str,
        mode: str,
        failed_ids: Optional[List] = None,
        new_titles: Optional[Dict] = None,
        id_to_name: Optional[Dict] = None,
        html_file_path: Optional[str] = None,
    ) -> bool:
        """Unified notification sending logic, including all condition checks"""
        has_notification = self._has_notification_configured()

        if (
            CONFIG["ENABLE_NOTIFICATION"]
            and has_notification
            and self._has_valid_content(stats, new_titles)
        ):
            send_to_notifications(
                stats,
                failed_ids or [],
                report_type,
                new_titles,
                id_to_name,
                self.update_info,
                self.proxy_url,
                mode=mode,
                html_file_path=html_file_path,
            )
            return True
        elif CONFIG["ENABLE_NOTIFICATION"] and not has_notification:
            print("⚠️ Warning: Notification enabled but no channels configured, skipping")
        elif not CONFIG["ENABLE_NOTIFICATION"]:
            print(f"Skipping {report_type} notification: Notification disabled")
        elif (
            CONFIG["ENABLE_NOTIFICATION"]
            and has_notification
            and not self._has_valid_content(stats, new_titles)
        ):
            mode_strategy = self._get_mode_strategy()
            if "Realtime" in report_type or "实时" in report_type:
                print(
                    f"Skipping realtime notification: No matching news in {mode_strategy['mode_name']}"
                )
            else:
                print(
                    f"Skipping {mode_strategy['summary_report_type']} notification: No valid news content matched"
                )

        return False

    def _generate_summary_report(self, mode_strategy: Dict) -> Optional[str]:
        """Generate summary report (with notification)"""
        summary_type = (
            "Current Ranking" if mode_strategy["summary_mode"] == "current" else "Daily Summary"
        )
        print(f"Generating {summary_type} report...")

        # Load analysis data
        analysis_data = self._load_analysis_data()
        if not analysis_data:
            return None

        all_results, id_to_name, title_info, new_titles, word_groups, filter_words = (
            analysis_data
        )

        # Run analysis pipeline
        stats, html_file = self._run_analysis_pipeline(
            all_results,
            mode_strategy["summary_mode"],
            title_info,
            new_titles,
            word_groups,
            filter_words,
            id_to_name,
            is_daily_summary=True,
        )

        print(f"{summary_type} report generated: {html_file}")

        # Send notification
        self._send_notification_if_needed(
            stats,
            mode_strategy["summary_report_type"],
            mode_strategy["summary_mode"],
            failed_ids=[],
            new_titles=new_titles,
            id_to_name=id_to_name,
            html_file_path=html_file,
        )

        return html_file

    def _generate_summary_html(self, mode: str = "daily") -> Optional[str]:
        """Generate summary HTML"""
        summary_type = "Current Ranking" if mode == "current" else "Daily Summary"
        print(f"Generating {summary_type} HTML...")

        # Load analysis data
        analysis_data = self._load_analysis_data()
        if not analysis_data:
            return None

        all_results, id_to_name, title_info, new_titles, word_groups, filter_words = (
            analysis_data
        )

        # Run analysis pipeline
        _, html_file = self._run_analysis_pipeline(
            all_results,
            mode,
            title_info,
            new_titles,
            word_groups,
            filter_words,
            id_to_name,
            is_daily_summary=True,
        )

        print(f"{summary_type} HTML generated: {html_file}")
        return html_file

    def _initialize_and_check_config(self) -> None:
        """General initialization and config check"""
        now = get_beijing_time()
        print(f"Current Beijing Time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

        if not CONFIG["ENABLE_CRAWLER"]:
            print("Crawler disabled (ENABLE_CRAWLER=False), exiting")
            return

        has_notification = self._has_notification_configured()
        if not CONFIG["ENABLE_NOTIFICATION"]:
            print("Notification disabled (ENABLE_NOTIFICATION=False), crawling only")
        elif not has_notification:
            print("No notification channels configured, crawling only")
        else:
            print("Notification enabled, will send notifications")

        mode_strategy = self._get_mode_strategy()
        print(f"Report Mode: {self.report_mode}")
        print(f"Run Mode: {mode_strategy['description']}")

    def _crawl_data(self) -> Tuple[Dict, Dict, List]:
        """Execute data crawling"""
        ids = []
        for platform in CONFIG["PLATFORMS"]:
            if "name" in platform:
                ids.append((platform["id"], platform["name"]))
            else:
                ids.append(platform["id"])

        print(
            f"Monitored Platforms: {[p.get('name', p['id']) for p in CONFIG['PLATFORMS']]}"
        )
        print(f"Starting crawl, interval {self.request_interval} ms")
        ensure_directory_exists("output")

        results, id_to_name, failed_ids = self.data_fetcher.crawl_websites(
            ids, self.request_interval
        )

        title_file = save_titles_to_file(results, id_to_name, failed_ids)
        print(f"Titles saved to: {title_file}")

        return results, id_to_name, failed_ids

    def _execute_mode_strategy(
        self, mode_strategy: Dict, results: Dict, id_to_name: Dict, failed_ids: List
    ) -> Optional[str]:
        """Execute mode specific logic"""
        # Get current monitoring platform ID list
        current_platform_ids = [platform["id"] for platform in CONFIG["PLATFORMS"]]

        new_titles = detect_latest_new_titles(current_platform_ids)
        time_info = Path(save_titles_to_file(results, id_to_name, failed_ids)).stem
        word_groups, filter_words = load_frequency_words()

        # current mode: realtime push needs to use complete historical data to ensure statistics integrity
        if self.report_mode == "current":
            # Load complete historical data (filtered by current platforms)
            analysis_data = self._load_analysis_data()
            if not analysis_data:
                print("❌ Critical Error: Cannot read newly saved data file")
                raise RuntimeError("Data consistency check failed: Read failed after save")

            all_results, historical_id_to_name, historical_title_info, historical_new_titles, _, _ = analysis_data

            print(
                f"current mode: using filtered historical data, platforms: {list(all_results.keys())}"
            )

            stats, html_file = self._run_analysis_pipeline(
                all_results,
                self.report_mode,
                historical_title_info,
                historical_new_titles,
                word_groups,
                filter_words,
                historical_id_to_name,
                failed_ids=failed_ids,
            )

            combined_id_to_name = {**historical_id_to_name, **id_to_name}

            print(f"HTML report generated: {html_file}")

            # Send realtime notification (using complete historical stats)
            summary_html = None
            if mode_strategy["should_send_realtime"]:
                self._send_notification_if_needed(
                    stats,
                    mode_strategy["realtime_report_type"],
                    self.report_mode,
                    failed_ids=failed_ids,
                    new_titles=historical_new_titles,
                    id_to_name=combined_id_to_name,
                    html_file_path=html_file,
                )
        else:
            title_info = self._prepare_current_title_info(results, time_info)
            stats, html_file = self._run_analysis_pipeline(
                results,
                self.report_mode,
                title_info,
                new_titles,
                word_groups,
                filter_words,
                id_to_name,
                failed_ids=failed_ids,
            )
            print(f"HTML report generated: {html_file}")

            # Send realtime notification (if needed)
            summary_html = None
            if mode_strategy["should_send_realtime"]:
                self._send_notification_if_needed(
                    stats,
                    mode_strategy["realtime_report_type"],
                    self.report_mode,
                    failed_ids=failed_ids,
                    new_titles=new_titles,
                    id_to_name=id_to_name,
                    html_file_path=html_file,
                )

        # Generate summary report (if needed)
        summary_html = None
        if mode_strategy["should_generate_summary"]:
            if mode_strategy["should_send_realtime"]:
                # If realtime notification sent, summary only generates HTML without notification
                summary_html = self._generate_summary_html(
                    mode_strategy["summary_mode"]
                )
            else:
                # daily mode: generate summary report and send notification
                summary_html = self._generate_summary_report(mode_strategy)

        # Open browser (non-container environment only)
        if self._should_open_browser() and html_file:
            if summary_html:
                summary_url = "file://" + str(Path(summary_html).resolve())
                print(f"Opening summary report: {summary_url}")
                webbrowser.open(summary_url)
            else:
                file_url = "file://" + str(Path(html_file).resolve())
                print(f"Opening HTML report: {file_url}")
                webbrowser.open(file_url)
        elif self.is_docker_container and html_file:
            if summary_html:
                print(f"Summary report generated (Docker): {summary_html}")
            else:
                print(f"HTML report generated (Docker): {html_file}")

        return summary_html

    def run(self) -> None:
        """Execute analysis process"""
        try:
            self._initialize_and_check_config()

            mode_strategy = self._get_mode_strategy()

            results, id_to_name, failed_ids = self._crawl_data()

            self._execute_mode_strategy(mode_strategy, results, id_to_name, failed_ids)

        except Exception as e:
            print(f"Analysis process failed: {e}")
            raise


def main():
    try:
        analyzer = NewsAnalyzer()
        analyzer.run()
    except FileNotFoundError as e:
        print(f"❌ Configuration Error: {e}")
        print("\nPlease ensure the following files exist:")
        print("  • config/config.yaml")
        print("  • config/frequency_words.txt")
        print("\nRefer to documentation for correct configuration")
    except Exception as e:
        print(f"❌ Runtime Error: {e}")
        raise


if __name__ == "__main__":
    main()
