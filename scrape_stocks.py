from playwright.sync_api import sync_playwright
import pandas as pd
import time
import json
import os
import re
import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==========================
# CONFIGURATION
# ==========================

SPREADSHEET_NAME = "PortfolioTrackerComet"   # Google Sheet file name
BASE_SHEET_NAME = "Sheet1"                   # Base tab with Stock Name / Symbol / Quantity
CREDENTIALS_FILE = "service-account.json"    # Will be created from GitHub secret in CI
COOKIES_FILE = "cookies.json"                # Used locally only (ignored across runs)
DELAY_BETWEEN_STOCKS = 10                    # seconds between each stock to avoid blocking

# Metric locators from Screener.in "top-ratios" section
metric_locators = {
    "ROCE": '//*[@id="top-ratios"]/li[7]/span[2]/span',
    "Intrinsic Value": '//*[@id="top-ratios"]/li[10]/span[2]/span',
    "ROE": '//*[@id="top-ratios"]/li[8]/span[2]/span',
    "Debt to Equity": '//*[@id="top-ratios"]/li[12]/span[2]/span',
    "PEG": '//*[@id="top-ratios"]/li[11]/span[2]/span',
    "Dividend Yield": '//*[@id="top-ratios"]/li[6]/span[2]/span',
    "Current Price": '//*[@id="top-ratios"]/li[2]/span[2]/span',
    "High": '//*[@id="top-ratios"]/li[3]/span[2]/span[1]',
    "Low": '//*[@id="top-ratios"]/li[3]/span[2]/span[2]',
    "Profit Var 10Yrs": '//*[@id="top-ratios"]/li[13]/span[2]/span',
    "Sales Var 10Yrs": '//*[@id="top-ratios"]/li[14]/span[2]/span',
}

# ==========================
# GOOGLE SHEETS SETUP
# ==========================

def get_gspread_client():
    scope = ["https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    return client

def get_weekly_sheet(spreadsheet, base_sheet_name: str):
    """Return worksheet for current week (YYYY-Www).
       If not exists, create it by copying structure & data from base sheet.
    """
    year, week_num, _ = datetime.date.today().isocalendar()
    weekly_title = f"{year}-W{week_num:02d}"
    print(f"📄 Weekly sheet for this run: {weekly_title}")

    try:
        ws = spreadsheet.worksheet(weekly_title)
        print(f"🔍 Found existing weekly sheet '{weekly_title}'")
        return ws, weekly_title
    except gspread.WorksheetNotFound:
        print(f"🆕 Weekly sheet '{weekly_title}' not found. Creating from base sheet '{base_sheet_name}'...")
        base_ws = spreadsheet.worksheet(base_sheet_name)
        all_values = base_ws.get_all_values()
        rows = str(max(len(all_values), 100))
        cols = str(max(len(all_values[0]) if all_values else 10, 10))

        ws = spreadsheet.add_worksheet(title=weekly_title, rows=rows, cols=cols)

        # Copy header and rows from base sheet if available
        if all_values:
            ws.update("A1", all_values)
            print(f"📎 Copied {len(all_values)} rows from base sheet '{base_sheet_name}'")
        else:
            print(f"⚠ Base sheet '{base_sheet_name}' is empty. Weekly sheet will start empty.")
        return ws, weekly_title

def load_weekly_dataframe(spreadsheet, base_sheet_name: str):
    weekly_ws, weekly_title = get_weekly_sheet(spreadsheet, base_sheet_name)

    records = weekly_ws.get_all_records()
    if records:
        df = pd.DataFrame(records)
        print(f"📥 Loaded {len(df)} rows from weekly sheet '{weekly_title}'")
    else:
        # If weekly sheet is empty, seed from base sheet
        base_ws = spreadsheet.worksheet(base_sheet_name)
        base_records = base_ws.get_all_records()
        df = pd.DataFrame(base_records)
        print(f"📥 Weekly sheet empty, loaded {len(df)} rows from base sheet '{base_sheet_name}'")

    df = df.fillna("")

    # Ensure required base columns exist
    for col in ["Stock Name", "Symbol", "Quantity"]:
        if col not in df.columns:
            df[col] = ""

    # Ensure metric columns exist
    for metric in metric_locators.keys():
        if metric not in df.columns:
            df[metric] = ""

    # Ensure LastUpdated column exists
    if "LastUpdated" not in df.columns:
        df["LastUpdated"] = ""

    return df, weekly_ws, weekly_title

# ==========================
# COOKIE & LOGIN MANAGEMENT
# ==========================

def save_cookies(context):
    try:
        cookies = context.cookies()
        with open(COOKIES_FILE, "w") as f:
            json.dump(cookies, f)
        print("🍪 Cookies saved (local run only).")
    except Exception as e:
        print(f"⚠ Failed to save cookies: {e}")

def load_cookies(context):
    if os.path.exists(COOKIES_FILE):
        try:
            with open(COOKIES_FILE, "r") as f:
                cookies = json.load(f)
            context.add_cookies(cookies)
            print("🔁 Local cookies loaded.")
            return True
        except Exception as e:
            print(f"⚠ Failed to load cookies: {e}")
    return False

def perform_login(page):
    """Auto login using Screener credentials from environment variables."""
    email = os.getenv("SCREENER_EMAIL")
    password = os.getenv("SCREENER_PASSWORD")

    if not email or not password:
        raise RuntimeError("SCREENER_EMAIL or SCREENER_PASSWORD environment variables are not set.")

    print("🔐 Performing auto-login to Screener.in ...")
    page.goto("https://www.screener.in/login/", timeout=60000)
    page.fill('//*[@id="id_username"]', email)
    page.fill('//*[@id="id_password"]', password)
    page.locator("button[type='submit']").click()
    page.wait_for_selector('#desktop-search input', timeout=60000)
    print("✅ Login successful.")

# ==========================
# SCRAPING UTILITIES
# ==========================

def convert_to_numeric(value: str):
    if not value:
        return None
    try:
        value = value.replace(",", "").replace("₹", "").strip()
        if "%" in value:
            return float(value.replace("%", "").strip())
        match = re.search(r"-?\d+\.?\d*", value)
        return float(match.group()) if match else None
    except Exception:
        return None

def get_metric_value(page, locator: str):
    try:
        element = page.locator(f"xpath={locator}")
        if element.is_visible():
            return element.inner_text().strip()
    except Exception:
        return None
    return None

# def scrape_with_retry(page, search_text: str, retry: int = 3):
#     for attempt in range(1, retry + 1):
#         try:
#             page.goto("https://www.screener.in/", timeout=60000)
#             page.fill('#desktop-search input', search_text)
#             page.keyboard.press("Enter")
#             page.wait_for_selector('//*[@id="top"]/div[1]/div/h1', timeout=60000)
#             print(f"✔ Loaded page for {search_text} (Attempt {attempt})")
#             return True
#         except Exception as e:
#             print(f"⚠ Attempt {attempt} failed for {search_text}: {e}")
#             if attempt < retry:
#                 time.sleep(2)
#     print(f"❌ Failed to load page for {search_text} after {retry} attempts.")
#     return False

def scrape_with_retry(page, search_text, retry=3):
    for attempt in range(1, retry + 1):
        try:
            # Go to screener homepage
            page.goto("https://www.screener.in/", timeout=60000)

            # Wait for search bar to be ready
            page.wait_for_selector('#desktop-search input', timeout=10000)

            # Extra small delay to allow JS to settle the autocomplete
            time.sleep(1)

            # Fill the search box
            page.fill('#desktop-search input', search_text)
            time.sleep(2)

            # Press enter
            page.keyboard.press("Enter")

            # Wait for the company title/header to appear (more stable selector)
            page.wait_for_selector('//*[@id="top"]/div[1]/div/h1', timeout=30000)

            print(f"✔ Loaded page successfully for {search_text} (Attempt {attempt})")
            return True

        except Exception as e:
            print(f"⚠ Attempt {attempt} failed for {search_text}: {e}")
            if attempt < retry:
                # Short retry wait
                time.sleep(5)

    print(f"❌ Failed to load page for {search_text} after {retry} attempts.")
    return False


def get_current_ist_timestamp():
    # IST = UTC + 5:30
    utc_now = datetime.datetime.utcnow()
    ist_now = utc_now + datetime.timedelta(hours=5, minutes=30)
    return ist_now.strftime("%Y-%m-%d %H:%M:%S IST")

# ==========================
# MAIN EXECUTION
# ==========================

def main():
    # Google Sheets client and weekly sheet/data
    client = get_gspread_client()
    spreadsheet = client.open(SPREADSHEET_NAME)
    df, weekly_ws, weekly_title = load_weekly_dataframe(spreadsheet, BASE_SHEET_NAME)

    with sync_playwright() as p:
        # For GitHub Actions and automation, use headless=True
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
                  record_video_dir="videos/",
                  record_video_size={"width": 1280, "height": 720},
        )

        page = context.new_page()

        # Try local cookies (only useful if you run locally)
        cookies_loaded = load_cookies(context)

        try:
            if not cookies_loaded:
                # Always perform auto-login in CI / cloud
                perform_login(page)
                save_cookies(context)

            # Iterate over each stock row
            for index, row in df.iterrows():
                symbol = str(row["Symbol"]).strip()
                if not symbol:
                    continue

                print(f"\n🔎 Processing symbol: {symbol}")

                if scrape_with_retry(page, symbol):
                    for metric, locator in metric_locators.items():
                        raw_value = get_metric_value(page, locator)
                        numeric_value = convert_to_numeric(raw_value)
                        df.loc[index, metric] = numeric_value if numeric_value is not None else raw_value
                        print(f"   🔄 {metric}: {df.loc[index, metric]}")

                    df.loc[index, "LastUpdated"] = get_current_ist_timestamp()
                    print(f"   🕒 LastUpdated: {df.loc[index, 'LastUpdated']}")
                else:
                    for metric in metric_locators.keys():
                        df.loc[index, metric] = "Error / Unavailable"
                    df.loc[index, "LastUpdated"] = get_current_ist_timestamp()
                    print(f"   ⚠ Marked row as Error / Unavailable")

                print(f"⏳ Waiting {DELAY_BETWEEN_STOCKS} seconds before next stock...")
                time.sleep(DELAY_BETWEEN_STOCKS)

        finally:
            context.close()
            browser.close()

    # Write updated DataFrame back to the weekly sheet
    print(f"\n📤 Writing updated data back to weekly sheet '{weekly_title}'...")
    # Clear old content and update with new header + data
    weekly_ws.clear()
    weekly_ws.update([df.columns.tolist()] + df.values.tolist())
    print("✅ Google Sheet updated successfully.")

if __name__ == "__main__":
    main()
