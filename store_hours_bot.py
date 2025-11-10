# ============= IMPORTS =============
import requests
import pandas as pd
import time
from io import StringIO
import openai
from tqdm import tqdm
import re
import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os

print("=" * 60)
print("SCRIPT STARTED - Testing output")
print("=" * 60)
print("Python script is running!")

# ============= CREDENTIALS (from environment variables) =============
MODE_TOKEN = os.environ.get('MODE_TOKEN')
MODE_SECRET = os.environ.get('MODE_SECRET')
MODE_ACCOUNT = 'doordash'
REPORT_ID = '8b50b0629b6b'
QUERY_ID = '036132875b62'

openai.api_key = os.environ.get('OPENAI_API_KEY')

SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
SLACK_CHANNEL_ID = 'C098G9URHEV'

print(f"✅ Loaded environment variables")
print(f"   MODE_TOKEN: {'Set' if MODE_TOKEN else 'MISSING'}")
print(f"   MODE_SECRET: {'Set' if MODE_SECRET else 'MISSING'}")
print(f"   OPENAI_API_KEY: {'Set' if openai.api_key else 'MISSING'}")
print(f"   SLACK_BOT_TOKEN: {'Set' if SLACK_BOT_TOKEN else 'MISSING'}")

# ============= CONFIGURATION =============
closure_categories = {
    "system issue": ["system", "technical", "pos", "payment", "network", "connectivity", "outage"],
    "maintenance issue": ["maintenance", "repair", "equipment", "electrical"],
    "weather issue": ["flood", "rain", "snow", "storm", "hurricane", "weather"],
    "emergency": ["emergency", "medical", "fire", "ambulance", "police", "safety"],
    "staffing issue": ["staff", "understaffed", "no employees", "short staffed", "sick callout"],
    "payment issue": ["cash only", "registers down", "no credit card", "credit cards not accepted", 
                      "card reader down", "cannot accept cards", "cash payment only"]
}

uncertain_phrases = [
    "i can't extract", "i'm unable to extract", "please check the image",
    "let me know", "based on your observations", "can't verify", "if you indicate",
    "choose from these options", "you might want to check", "depending on", "select",
    "faint", "obstructed", "too small", "unclear signage", "partially visible",
    "blurry", "low resolution", "hard to read", "illegible", "glare", "shadow"
]

permanent_closure_phrases = [
    "permanently closing", "closed permanently", "permanent closure",
    "permanently closed", "closing permanently", "will be permanently closing",
    "this location is now permanently closed", "store closing"
]

# STRICTER: Only very explicit address change phrases
address_change_phrases = [
    "we are moving to", "we have moved to", "we've moved to",
    "relocated to", "new location:", "new address:",
    "moved to:", "find us at our new location"
]

# Long-term temporary closure phrases
long_term_closure_phrases = [
    "closed until further notice", "until further notice", "closed indefinitely",
    "temporarily closed until further notice"
]

# Payment system issues
payment_issue_phrases = [
    "cash only", "registers down", "no credit card", "credit cards not accepted",
    "card reader down", "cannot accept cards", "cash payment only",
    "no cards accepted", "credit card machine down", "debit cards not accepted"
]

# Holiday keywords for special hours detection
holiday_keywords = {
    "thanksgiving": ["thanksgiving"],
    "black friday": ["black friday"],
    "christmas": ["christmas", "holiday"],
    "new year": ["new year", "new year's"],
    "easter": ["easter"],
    "labor day": ["labor day"],
    "memorial day": ["memorial day"],
    "july 4th": ["july 4", "independence day"],
    "independence day": ["independence day"],
    "halloween": ["halloween"],
    "cyber monday": ["cyber monday"],
    "mother's day": ["mother's day"],
    "father's day": ["father's day"],
    "valentine's day": ["valentine's day"],
    "st. patrick's day": ["st. patrick", "patrick's day"]
}

# ============= HELPER FUNCTIONS =============
def categorize_closure(text):
    lower = text.lower()
    for category, terms in closure_categories.items():
        if any(t in lower for t in terms):
            return category
    return "other"

def extract_new_address(text):
    """Extract new address from text if mentioned"""
    lower = text.lower()
    
    # Look for address patterns after relocation phrases
    for phrase in address_change_phrases:
        if phrase in lower:
            # Try to find address pattern: numbers + street
            address_pattern = r"(?:new address:|new location:|moved to:|find us at:)?\s*(\d+\s+[A-Za-z0-9\s,\.]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct)[A-Za-z0-9\s,\.]*)"
            match = re.search(address_pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
    
    return ""

def extract_hours(text):
    hours = {}
    pattern = r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday)[^\n]*?(\d{1,2}:\d{2}(?:\s*[ap]m)?)\s*[-–]\s*(\d{1,2}:\d{2}(?:\s*[ap]m)?)"
    for day, start, end in re.findall(pattern, text, re.IGNORECASE):
        try:
            start_clean = start.strip().lower().replace(" ", "")
            end_clean = end.strip().lower().replace(" ", "")
            if not ("am" in start_clean or "pm" in start_clean):
                start_clean += "am"
            if not ("am" in end_clean or "pm" in end_clean):
                end_clean += "pm"
            if end_clean == "12:00pm":
                e = "00:00:00"
            else:
                e = datetime.datetime.strptime(end_clean, "%I:%M%p").strftime("%H:%M:%S")
            s = datetime.datetime.strptime(start_clean, "%I:%M%p").strftime("%H:%M:%S")
            hours[day.lower()] = {"start": s, "end": e}
        except:
            try:
                s = datetime.datetime.strptime(start.strip(), "%H:%M").strftime("%H:%M:%S")
                e = datetime.datetime.strptime(end.strip(), "%H:%M").strftime("%H:%M:%S")
                hours[day.lower()] = {"start": s, "end": e}
            except:
                continue
    return hours

def extract_special_hours(text):
    """Extract special holiday hours from text. Looks for SPECIAL HOLIDAY HOURS: section"""
    special_hours = []
    
    # First, try to find the SPECIAL HOLIDAY HOURS: section
    special_section_match = re.search(
        r'SPECIAL\s+HOLIDAY\s+HOURS\s*:\s*(.*?)(?:\n\n|\Z)',
        text,
        re.IGNORECASE | re.DOTALL
    )
    
    if not special_section_match:
        # Fallback: search the entire text line by line (for cases where format isn't perfect)
        lines = text.split('\n')
    else:
        # Parse from the special section
        section_text = special_section_match.group(1)
        lines = section_text.split('\n')
    
    # Holiday patterns - ORDER MATTERS! Check specific holidays (with "Eve") BEFORE generic ones
    holidays_to_check = [
        ('thanksgiving', ['thanksgiving']),
        ('black friday', ['black friday']),
        ('christmas eve', ['christmas eve', "christmas's eve"]),
        ('christmas day', ['christmas day']),
        ('christmas', ['christmas']),
        ("new year's eve", ["new year's eve", 'new years eve', 'new year eve']),
        ("new year's day", ["new year's day", 'new years day', 'new year day']),
        ('easter', ['easter']),
        ('labor day', ['labor day']),
        ('memorial day', ['memorial day']),
        ('july 4th', ['july 4th', 'july 4', 'independence day']),
        ('halloween', ['halloween']),
        ('cyber monday', ['cyber monday']),
        ("mother's day", ["mother's day", 'mothers day']),
        ("father's day", ["father's day", 'fathers day']),
        ("valentine's day", ["valentine's day", 'valentines day']),
        ("st. patrick's day", ["st. patrick's day", 'st. patrick', "patrick's day"]),
    ]
    
    for line in lines:
        if not line.strip() or ':' not in line:
            continue
        
        lower_line = line.lower()
        
        # Find which holiday is in this line
        found_holiday = None
        for canonical_name, keywords in holidays_to_check:
            for keyword in keywords:
                if keyword in lower_line:
                    found_holiday = canonical_name
                    break
            if found_holiday:
                break
        
        if not found_holiday:
            continue
        
        # Extract the part after the colon
        parts = line.split(':', 1)
        if len(parts) < 2:
            continue
        
        status_part = parts[1].strip()
        
        # Check if closed
        if any(phrase in status_part.lower() for phrase in ['closed', 'close at', 'closes at', 'no operating', 'no hours']):
            special_hours.append({
                'holiday': found_holiday,
                'is_open': 'no',
                'start_time': '',
                'end_time': ''
            })
            continue
        
        # Try to parse times
        # Handle formats like: 
        # - "09:00-14:00" (24-hour, primary format)
        # - "9:00-14:00"
        # - "Open-4PM", "09:00-16:00"
        # - "8:00 AM - 9:00 PM" (legacy 12-hour format)
        
        # First, normalize to 24-hour format if using AM/PM
        if 'am' in status_part.lower() or 'pm' in status_part.lower():
            # Remove "open" prefix and normalize
            time_str = re.sub(r'^\s*open\s*[-–]?\s*', '', status_part, flags=re.IGNORECASE).strip()
            
            # Try full time range: "8:00 AM - 9:00 PM" or "8AM-9PM"
            full_range_match = re.search(
                r'(\d{1,2}):?(\d{2})?\s*([ap]m)?\s*[-–]\s*(\d{1,2}):?(\d{2})?\s*([ap]m)?',
                time_str,
                re.IGNORECASE
            )
            
            if full_range_match:
                try:
                    s_hr, s_min, s_ap, e_hr, e_min, e_ap = full_range_match.groups()
                    s_min = s_min or '00'
                    e_min = e_min or '00'
                    
                    # Smart AM/PM assignment
                    if not s_ap and not e_ap:
                        s_ap = 'am'
                        e_ap = 'pm'
                    elif not s_ap:
                        s_ap = e_ap
                    elif not e_ap:
                        e_ap = 'pm' if int(e_hr) < int(s_hr) else s_ap
                    
                    start_dt = datetime.datetime.strptime(f"{s_hr}:{s_min}{s_ap}".lower(), "%I:%M%p")
                    end_dt = datetime.datetime.strptime(f"{e_hr}:{e_min}{e_ap}".lower(), "%I:%M%p")
                    
                    special_hours.append({
                        'holiday': found_holiday,
                        'is_open': 'yes',
                        'start_time': start_dt.strftime("%H:%M"),
                        'end_time': end_dt.strftime("%H:%M")
                    })
                    continue
                except:
                    pass
            
            # Try single time (closing time only): "4PM", "4 PM"
            single_time_match = re.search(
                r'(\d{1,2}):?(\d{2})?\s*([ap]m)?',
                time_str,
                re.IGNORECASE
            )
            
            if single_time_match:
                try:
                    hr, minute, ap = single_time_match.groups()
                    minute = minute or '00'
                    ap = ap or 'pm'
                    
                    end_dt = datetime.datetime.strptime(f"{hr}:{minute}{ap}".lower(), "%I:%M%p")
                    
                    special_hours.append({
                        'holiday': found_holiday,
                        'is_open': 'yes',
                        'start_time': '09:00',
                        'end_time': end_dt.strftime("%H:%M")
                    })
                    continue
                except:
                    pass
        else:
            # Try 24-hour format: "09:00-14:00" or "9:00-14:00"
            time_24h_match = re.search(
                r'(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})',
                status_part
            )
            
            if time_24h_match:
                try:
                    s_hr, s_min, e_hr, e_min = time_24h_match.groups()
                    start_dt = datetime.datetime.strptime(f"{s_hr}:{s_min}", "%H:%M")
                    end_dt = datetime.datetime.strptime(f"{e_hr}:{e_min}", "%H:%M")
                    
                    special_hours.append({
                        'holiday': found_holiday,
                        'is_open': 'yes',
                        'start_time': start_dt.strftime("%H:%M"),
                        'end_time': end_dt.strftime("%H:%M")
                    })
                    continue
                except:
                    pass
            
            # Try single 24-hour time: "14:00"
            single_24h_match = re.search(r'(\d{1,2}):(\d{2})', status_part)
            if single_24h_match:
                try:
                    hr, minute = single_24h_match.groups()
                    end_dt = datetime.datetime.strptime(f"{hr}:{minute}", "%H:%M")
                    
                    special_hours.append({
                        'holiday': found_holiday,
                        'is_open': 'yes',
                        'start_time': '09:00',
                        'end_time': end_dt.strftime("%H:%M")
                    })
                    continue
                except:
                    pass
    
    return special_hours

def get_holiday_date(holiday_name, year=2025):
    """Get the date for a given holiday"""
    lower_holiday = holiday_name.lower()
    
    # Fixed holidays
    if "thanksgiving" in lower_holiday:
        # Thanksgiving is 4th Thursday of November
        date = datetime.date(year, 11, 1)
        thursdays = [d for d in range(22, 29) if datetime.date(year, 11, d).weekday() == 3]
        return datetime.date(year, 11, thursdays[3]) if len(thursdays) > 3 else None
    elif "black friday" in lower_holiday:
        # Day after Thanksgiving
        date = datetime.date(year, 11, 1)
        thursdays = [d for d in range(22, 29) if datetime.date(year, 11, d).weekday() == 3]
        if len(thursdays) > 3:
            return datetime.date(year, 11, thursdays[3] + 1)
    elif "christmas" in lower_holiday:
        return datetime.date(year, 12, 25)
    elif "new year" in lower_holiday:
        return datetime.date(year + 1, 1, 1)
    elif "easter" in lower_holiday:
        # Easter calculation (simplified - you may want a proper library)
        # For 2025, Easter is April 20
        return datetime.date(year, 4, 20) if year == 2025 else None
    elif "labor day" in lower_holiday:
        # First Monday of September
        date = datetime.date(year, 9, 1)
        while date.weekday() != 0:
            date += datetime.timedelta(days=1)
        return date
    elif "memorial day" in lower_holiday:
        # Last Monday of May
        date = datetime.date(year, 5, 31)
        while date.weekday() != 0:
            date -= datetime.timedelta(days=1)
        return date
    elif "july 4" in lower_holiday or "independence day" in lower_holiday:
        return datetime.date(year, 7, 4)
    elif "halloween" in lower_holiday:
        return datetime.date(year, 10, 31)
    elif "mother's day" in lower_holiday:
        # Second Sunday of May
        date = datetime.date(year, 5, 1)
        sundays = 0
        while sundays < 2:
            if date.weekday() == 6:
                sundays += 1
            if sundays < 2:
                date += datetime.timedelta(days=1)
        return date
    elif "father's day" in lower_holiday:
        # Third Sunday of June
        date = datetime.date(year, 6, 1)
        sundays = 0
        while sundays < 3:
            if date.weekday() == 6:
                sundays += 1
            if sundays < 3:
                date += datetime.timedelta(days=1)
        return date
    elif "valentine's day" in lower_holiday:
        return datetime.date(year, 2, 14)
    elif "cyber monday" in lower_holiday:
        # Monday after Black Friday (Thanksgiving + 3 days)
        date = datetime.date(year, 11, 1)
        thursdays = [d for d in range(22, 29) if datetime.date(year, 11, d).weekday() == 3]
        if len(thursdays) > 3:
            return datetime.date(year, 11, thursdays[3] + 3)
    elif "st. patrick" in lower_holiday or "patrick's day" in lower_holiday:
        return datetime.date(year, 3, 17)
    
    return None

def normalize_time(t):
    if not t or not isinstance(t, str) or not re.match(r"\d{1,2}:\d{2}:\d{2}", t):
        return ""
    if t == "00:00:00":
        return "23:59:59"
    try:
        return datetime.datetime.strptime(t, "%H:%M:%S").strftime("%H:%M:%S")
    except:
        return t

def time_diff_min(t1, t2):
    dt1 = datetime.datetime.strptime(t1, "%H:%M:%S")
    dt2 = datetime.datetime.strptime(t2, "%H:%M:%S")
    delta = abs((dt1 - dt2).total_seconds())
    return min(delta, 86400 - delta) / 60

def confidence_from_hours(posted_hours_dict):
    valid_days = 0
    for v in posted_hours_dict.values():
        if v.get("start") and v.get("end") and re.match(r"^\d{2}:\d{2}:\d{2}$", v["start"]) and re.match(r"^\d{2}:\d{2}:\d{2}$", v["end"]):
            valid_days += 1
    return round(min(max(valid_days / 7.0, 0.0), 1.0), 2)

def extract_clarity_score(text):
    # Updated to preserve 2 decimal places
    m = re.search(r"clarity\s*score\s*[:\-]\s*(1(?:\.0+)?|0\.\d+|\.\d+)", text, re.IGNORECASE)
    if m:
        try:
            score = float(m.group(1))
            return round(score, 2)  # Ensure 2 decimal places
        except:
            pass
    if any(p in text.lower() for p in uncertain_phrases):
        return 0.20
    return 0.60

def hour_change_confidence(parse_coverage, clarity):
    """Confidence score specifically for hour changes"""
    return round(max(0.0, min(1.0, 0.6*parse_coverage + 0.4*clarity)), 2)

def is_permanent_closure(text):
    """Check if the text indicates a permanent closure"""
    lower = text.lower()
    
    # First check if it's a long-term temp closure (takes precedence)
    if any(phrase in lower for phrase in long_term_closure_phrases):
        return False
    
    strong_indicators = [
        "permanently closing", "closed permanently", "permanent closure",
        "permanently closed", "closing permanently", "will be permanently closing",
        "this location is now permanently closed"
    ]
    
    for phrase in strong_indicators:
        if phrase in lower:
            return True
    
    if "store closing" in lower:
        permanent_context = [
            "final", "last day", "we are closing",
            "location is closing", "this store is closing"
        ]
        if any(ctx in lower for ctx in permanent_context):
            return True
    
    return False

def is_long_term_closure(text):
    """Check if the text indicates a long-term temporary closure (until further notice)"""
    lower = text.lower()
    return any(phrase in lower for phrase in long_term_closure_phrases)

def is_payment_issue(text):
    """Check if the text indicates payment system issues"""
    lower = text.lower()
    return any(phrase in lower for phrase in payment_issue_phrases)

def is_hours_hallucination(text, extracted_hours_count):
    """Detect if GPT hallucinated store hours without actually seeing them in the image"""
    lower = text.lower()
    
    # Red flags for hallucination
    hallucination_indicators = [
        "no store hours visible",
        "cannot see store hours",
        "unable to read store hours",
        "no hours sign",
        "hours not visible",
        "no posted hours"
    ]
    
    # If GPT explicitly says no hours are visible, but extracted hours anyway, that's hallucination
    if any(indicator in lower for indicator in hallucination_indicators):
        if extracted_hours_count > 0:
            return True
    
    # Positive indicators that GPT actually found hours
    location_phrases = [
        "on the door",
        "on the window",
        "on the wall",
        "posted on",
        "sign shows",
        "sign reads",
        "sign displays",
        "visible on",
        "displayed on",
        "laminated",
        "chalkboard",
        "digital display",
        "next to",
        "above the",
        "below the",
        "frame",
        "entrance",
        "quote:",
        "says:",
        "reads:"
    ]
    
    has_location_description = any(phrase in lower for phrase in location_phrases)
    
    # If high hours extraction but no location description, suspicious
    if extracted_hours_count >= 4 and not has_location_description:
        return True
    
    return False

# ============= FUNCTION 1: GET DATA FROM MODE =============
def get_mode_data():
    print("\n🔄 Fetching data from Mode...")
    
    run_url = f'https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs'
    response = requests.post(run_url, auth=(MODE_TOKEN, MODE_SECRET))
    run_token = response.json()['token']
    print(f"✅ Run started: {run_token}")
    
    state_url = f'https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs/{run_token}'
    while True:
        response = requests.get(state_url, auth=(MODE_TOKEN, MODE_SECRET))
        state = response.json()['state']
        if state == 'succeeded':
            print("✅ Query completed!")
            break
        elif state in ['failed', 'cancelled']:
            raise Exception(f"Mode query {state}")
        print(f"   Waiting... ({state})")
        time.sleep(5)
    
    query_runs_url = f'https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs/{run_token}/query_runs'
    response = requests.get(query_runs_url, auth=(MODE_TOKEN, MODE_SECRET))
    query_runs = response.json()['_embedded']['query_runs']
    
    query_run_token = None
    for qr in query_runs:
        if qr['query_token'] == QUERY_ID:
            query_run_token = qr['token']
            break
    
    if not query_run_token:
        raise Exception("Could not find query run token")
    
    result_url = f'https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs/{run_token}/query_runs/{query_run_token}/results/content.csv'
    csv_response = requests.get(result_url, auth=(MODE_TOKEN, MODE_SECRET))
    df = pd.read_csv(StringIO(csv_response.text))
    
    # DEDUPLICATE: Keep only one entry per store (most recent if timestamp available)
    original_count = len(df)
    if 'STORE_ID' in df.columns:
        # Try to sort by timestamp if available
        timestamp_cols = [col for col in df.columns if 'TIME' in col.upper() or 'DATE' in col.upper() or 'CREATED' in col.upper()]
        if timestamp_cols:
            try:
                df = df.sort_values(timestamp_cols[0], ascending=False)
            except:
                pass
        
        df = df.drop_duplicates(subset='STORE_ID', keep='first')
        
        if len(df) < original_count:
            print(f"⚠️  Removed {original_count - len(df)} duplicate stores (kept most recent)")
    
    print(f"✅ Retrieved {len(df)} unique stores\n")
    return df

# ============= FUNCTION 2: PROCESS WITH OPENAI =============
def process_store_hours(df):
    print("\n🤖 Processing with OpenAI vision API...")
    
    recommendations, reasons, summary_reasons = [], [], []
    deactivation_reason_id, is_temp_deactivation = [], []
    confidence_scores = []
    new_addresses = []
    temp_duration = []
    special_hours_list = []  # NEW: Track special hours for each store
    
    bulk_hours = {day: {"start": [], "end": []} for day in [
        "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
    ]}
    
    # RELAXED: Only check for severe quality issues
    severe_quality_issues = [
        "cannot read", "illegible", "unreadable", "too blurry",
        "cannot make out", "unable to read"
    ]
    
    for i, row in tqdm(df.iterrows(), total=len(df)):
        image_url = row.get("IMAGE_URL")
        store_hours = str(row.get("STORE_HOURS", ""))

        if not image_url or not store_hours:
            recommendations.append("No change")
            reasons.append("Missing image or hours")
            summary_reasons.append("Missing image or hours")
            deactivation_reason_id.append("")
            is_temp_deactivation.append(False)
            confidence_scores.append(0.0)
            new_addresses.append("")
            temp_duration.append("")
            special_hours_list.append([])  # NEW
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
            continue

        prompt = f"""
You are reviewing a Dasher photo of a store entrance. CRITICAL: Check for closure and relocation signs FIRST before trying to read store hours.

SPECIAL ATTENTION - CHECK FOR HOLIDAY/SPECIAL HOURS SIGNS:
Before processing regular hours, look for signs indicating special hours for upcoming holidays or special dates:
- Thanksgiving, Black Friday, Christmas, New Year's, Easter, Labor Day, etc.
- Format like: "Thanksgiving: CLOSED" or "Black Friday: 8:00 AM - 9:00 PM"
- List ALL special holiday hours you can read clearly

PRIORITY ORDER (check in this order):
1) Is there a RELOCATION/ADDRESS CHANGE sign?

CRITICAL - Address Change Detection Rules:
✅ ONLY flag as "Address Change" if the sign EXPLICITLY says THIS STORE is moving:
   - "WE ARE MOVING TO [new address]"
   - "NEW LOCATION: [address]"
   - "RELOCATED TO [address]"
   - "OUR NEW ADDRESS IS [address]"
   - "THIS STORE HAS MOVED TO [address]"
   
❌ DO NOT flag as "Address Change" if:
   - Sign says "temporarily closed" + "visit another store near you" (this is NOT moving, just directing to other stores)
   - Sign says "find a store locator" or "locate another store" (this is NOT moving)
   - Sign says "reopen soon" or "will reopen" (this means SAME location will reopen, NOT moving)
   - Sign directs to website to find OTHER stores (this is NOT moving, just customer service)
   - Sign says "apologize for inconvenience" + visit other locations (this is temp closure, NOT moving)
   
"Address Change" means THIS SPECIFIC STORE is moving to a new address, NOT that customers should visit a different existing store.

2) Is there a LONG-TERM TEMPORARY closure sign? (e.g., "Closed until further notice", "Temporarily closed until further notice")
3) Is there a PERMANENT closure sign? (e.g., "permanently closing", "closed permanently")
4) Is there a PAYMENT SYSTEM issue? (e.g., "CASH ONLY", "Registers Down", "No Credit Cards", "Card Reader Down")
5) Is there a TEMPORARY closure sign? (e.g., "POWER OUT", "NO POWER", "closed due to weather", "system down", "maintenance", "closed today", "open in X minutes")
6) Are the posted store hours clearly visible AND readable?

Choose ONE recommendation:
- **Address Change** - ONLY if THIS STORE is explicitly moving to a new address with the new address shown or clearly stated
- **Temporarily Close For Day - Long Term** - If you see "closed until further notice" or "closed indefinitely" (no specific reopening date)
- **Permanently Close Store** - ONLY if you see clear permanent closure signage
- **Temporarily Close For Day** - If you see ANY of these:
  * Payment issues: "Cash Only", "Registers Down", "No Credit Cards", "Card Reader Down"
  * Power/system issues: "NO POWER", "POWER OUT", "System Down"
  * Temporary closure: maintenance, weather, closed for the day
  * Opening soon: "Open in X minutes", "Opening soon"
- **Change Store Hours** - If there are NO closure/relocation signs AND you can read the hours with reasonable confidence
- **No Change** - If hours are completely unreadable OR match DoorDash hours

IMPORTANT: 
- For "Address Change", THIS STORE must be moving to a new address - not just directing customers to other stores
- "Temporarily closed, visit another store" = Temp closure, NOT address change
- "Closed until further notice" should be flagged as "Temporarily Close For Day - Long Term", NOT permanent closure
- Payment issues like "Cash Only" or "Registers Down" should ALWAYS be flagged as "Temporarily Close For Day" because DoorDash requires card payments
- Signs like "NO POWER" or "Open in 45 minutes" should be flagged as "Temporarily Close For Day"
- Only recommend "No Change" if you literally CANNOT read the hours at all or if they match DoorDash hours

Current DoorDash hours: {store_hours}

CRITICAL RULES FOR READING STORE HOURS:
- You MUST be able to read BOTH opening time AND closing time for each day
- If you can only see closing times (e.g., "Closes at 9 PM") but NOT opening times, DO NOT recommend "Change Store Hours"
- If you can only see opening times but NOT closing times, DO NOT recommend "Change Store Hours"
- Only recommend hour changes if BOTH opening and closing times are clearly visible for at least 4 days

CRITICAL - IF YOU ARE READING STORE HOURS, YOU MUST ALSO:
1. Describe the physical location of the hours sign (e.g., "on the door frame", "posted on the window", "on the wall next to the entrance")
2. Describe what the sign looks like (e.g., "laminated white sign", "chalkboard", "digital display")
3. Quote the exact text you see (e.g., "Monday-Friday 9:00 AM - 9:00 PM")
4. If you CANNOT see hours at all, clearly state: "NO STORE HOURS VISIBLE"

Examples of what NOT to do:
❌ Sign shows "Closes at 9 PM" → DO NOT assume opening time → DO NOT recommend hour change
❌ Sign shows "Open 8 AM" → DO NOT assume closing time → DO NOT recommend hour change
❌ Extract hours without describing where the sign is located (likely hallucination)
✅ Sign shows "Monday: 8 AM - 9 PM" on a white sign next to the door → Both times visible → Can recommend change
✅ State clearly: "NO STORE HOURS VISIBLE on this storefront"

If recommending changed hours, list the full weekly schedule with BOTH opening and closing times clearly (e.g. Monday: 08:00 - 22:00).

If you detect an address change, include a line:
New Address: [the new address if visible, or "Not shown" if not visible]

CRITICAL - SPECIAL HOLIDAY HOURS AT END:
If you see special hours for any holidays, you MUST include a separate section at the very end of your response (after Clarity score) with each holiday on its own line:

SPECIAL HOLIDAY HOURS:
[Holiday Name]: [CLOSED or HH:MM-HH:MM]

Examples:
SPECIAL HOLIDAY HOURS:
Thanksgiving: Closed
Christmas Eve: Open-14:00
Christmas Day: Closed
New Year's Eve: Open-19:00
New Year's Day: Closed

CRITICAL RULES FOR SPECIAL HOURS:
- Each holiday on a separate line
- Use exact holiday names: "Thanksgiving", "Christmas Eve", "New Year's Eve", "Christmas Day", "New Year's Day", "Black Friday", etc.
- Time format: Use 24-hour format (14:00 not 2:00 PM)
- For closed stores: write "Closed" not "Close at X" or "Closes at X"
- For open stores: write the full range "HH:MM-HH:MM" or if only closing time known, write "09:00-HH:MM"
- This section MUST be at the very end of your response after the Clarity score
- Do NOT include dates or day-of-week info, just the time

CRITICAL - CLARITY SCORING INSTRUCTIONS:
Rate clarity based ONLY on the specific sign relevant to your recommendation:

- If recommending "Temporarily Close For Day" → Rate ONLY the closure sign clarity (e.g., "Open in 45 Minutes", "NO POWER", "Closed Today")
- If recommending "Change Store Hours" → Rate ONLY the store hours sign clarity
- If recommending "Permanently Close Store" → Rate ONLY the permanent closure sign clarity
- If recommending "Address Change" → Rate ONLY the relocation sign clarity

IGNORE when rating clarity:
- Overall image quality/glare/reflections on windows
- Clarity of other unrelated signs in the image
- Background visibility
- Other papers or stickers

Focus ONLY on: "Can I clearly read the specific sign that led to my recommendation?"

At the end, provide:
Clarity score: X.XX (rating ONLY the specific sign I'm reading for my recommendation)
Where X.XX is a number between 0.00 and 1.00 with TWO decimal places.
"""
        prompt += "\nAssume store closing times like '10:00' or '12:00' without AM/PM are in the evening (PM)."

        try:
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]}
                ],
                max_tokens=1000
            )

            result = response.choices[0].message.content.strip()
            reason = result
            lower = result.lower()

            # NEW: Extract special holiday hours
            special_hours_extracted = extract_special_hours(result)
            special_hours_list.append(special_hours_extracted)

            posted = extract_hours(result)
            parse_coverage = confidence_from_hours(posted)
            clarity = extract_clarity_score(result)

            # Check for uncertainty phrases first
            if any(p in lower for p in uncertain_phrases):
                recommendations.append("No change")
                reasons.append("Model expressed uncertainty")
                summary_reasons.append("Image unreadable or GPT uncertain")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(0.20)
                new_addresses.append("")
                temp_duration.append("")
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # RELAXED: Only check for SEVERE quality issues
            has_severe_quality_issues = any(phrase in lower for phrase in severe_quality_issues)

            # PRIORITY 0: Check for ADDRESS CHANGE - VERY STRICT
            if is_address_change(result):
                # Extra validation: make sure it's not just "visit another store" type language
                false_positive_phrases = [
                    "visit another store", "find another store", "locate another store",
                    "temporarily closed", "will reopen", "reopen soon",
                    "apologize for", "store near you", "store locator"
                ]
                
                is_false_positive = any(phrase in lower for phrase in false_positive_phrases)
                
                if is_false_positive:
                    # This is actually a temp closure, not address change
                    if clarity < 0.75:
                        recommendations.append("No change")
                        reasons.append(f"Clarity too low ({clarity:.2f} < 0.75)")
                        summary_reasons.append("Clarity too low")
                        deactivation_reason_id.append("")
                        is_temp_deactivation.append(False)
                        confidence_scores.append(clarity)
                        new_addresses.append("")
                        temp_duration.append("")
                        for day in bulk_hours:
                            bulk_hours[day]["start"].append("")
                            bulk_hours[day]["end"].append("")
                        continue
                    
                    # It's a temp closure
                    recommendations.append("Temporarily Close For Day")
                    reasons.append(reason)
                    summary_reasons.append("Temporarily closed - directing customers to other stores")
                    deactivation_reason_id.append("67")
                    is_temp_deactivation.append(True)
                    confidence_scores.append(max(0.80, clarity))
                    new_addresses.append("")
                    temp_duration.append(700)  # Long-term temp closure
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                # Address changes need highest clarity (0.92+)
                if clarity < 0.92:
                    recommendations.append("No change")
                    reasons.append(f"Clarity of relocation sign too low ({clarity:.2f} < 0.92)")
                    summary_reasons.append("Clarity too low for address change")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                new_addr = extract_new_address(result)
                # Additional validation: must have extracted an address OR very explicit language
                explicit_relocation = any(phrase in lower for phrase in [
                    "we have moved to", "we are moving to", "relocated to", "new location:", "new address:"
                ])
                
                if new_addr or explicit_relocation:
                    recommendations.append("Address Change")
                    reasons.append(reason)
                    summary_reasons.append("Store relocation/address change detected")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(max(0.85, clarity))
                    new_addresses.append(new_addr)
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue

            # PRIORITY 0.5: Check for LONG-TERM TEMPORARY closure
            if is_long_term_closure(result) or "long term" in lower:
                # Long-term closures need medium clarity (0.75+)
                if clarity < 0.75:
                    recommendations.append("No change")
                    reasons.append(f"Clarity of closure sign too low ({clarity:.2f} < 0.75)")
                    summary_reasons.append("Clarity too low for long-term closure")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append("Closed until further notice")
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.85, clarity))
                new_addresses.append("")
                temp_duration.append(700)  # 700 hours = ~1 month
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 1: Check for PERMANENT closure
            if "permanently close" in lower and is_permanent_closure(result):
                # Permanent closures need high clarity (0.85+)
                if clarity < 0.85:
                    recommendations.append("No change")
                    reasons.append(f"Clarity of permanent closure sign too low ({clarity:.2f} < 0.85)")
                    summary_reasons.append("Clarity too low for permanent closure")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                recommendations.append("Permanently Close Store")
                reasons.append(reason)
                summary_reasons.append("Permanent closure detected")
                deactivation_reason_id.append("23")
                is_temp_deactivation.append(False)
                confidence_scores.append(0.95)
                new_addresses.append("")
                temp_duration.append("")
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 1.5: Check for PAYMENT ISSUES
            if is_payment_issue(result):
                # Payment issues need low clarity (0.75+) - signs are usually clear
                if clarity < 0.75:
                    recommendations.append("No change")
                    reasons.append(f"Clarity of payment issue sign too low ({clarity:.2f} < 0.75)")
                    summary_reasons.append("Clarity too low for payment issue")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append("Payment issue")
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.80, clarity))
                new_addresses.append("")
                temp_duration.append(12)  # Regular temp closure = 12 hours
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 2: Check for regular TEMPORARY closure
            # IMPORTANT: Don't flag as temp closure if GPT already recommended "No Change" (special hours only)
            if "temporarily close" in lower and "recommendation: **no change**" not in lower:
                # Temp closures need low clarity (0.75+) - closure signs usually clear
                if clarity < 0.75:
                    recommendations.append("No change")
                    reasons.append(f"Clarity of closure sign too low ({clarity:.2f} < 0.75)")
                    summary_reasons.append("Clarity too low for temp closure")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append(categorize_closure(lower))
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.80, clarity))
                new_addresses.append("")
                temp_duration.append(12)  # Regular temp closure = 12 hours
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # Additional temp closure checks
            if (any(phrase in lower for phrase in [
                "closed for the day", "closed today", "closed due to", "store is closed",
                "power out", "no power", "maintenance", "system down", "open in", "opening in"
            ]) and "permanently" not in lower):
                # Temp closures need low clarity (0.75+)
                if clarity < 0.75:
                    recommendations.append("No change")
                    reasons.append(f"Clarity of closure sign too low ({clarity:.2f} < 0.75)")
                    summary_reasons.append("Clarity too low for temp closure")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append(categorize_closure(lower))
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.80, clarity))
                new_addresses.append("")
                temp_duration.append(12)  # Regular temp closure = 12 hours
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 3: Only NOW check for hour changes
            if "recommend" in lower and "change store hour" in lower:
                # Hour changes need HIGH clarity (0.90+) - must read exact times
                if clarity < 0.90:
                    recommendations.append("No change")
                    reasons.append(f"Clarity of hours sign too low for hour changes ({clarity:.2f} < 0.90)")
                    summary_reasons.append("Clarity too low for hour changes")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                # NEW: Check for hallucination (GPT making up hours)
                if is_hours_hallucination(result, len(posted)):
                    recommendations.append("No change")
                    reasons.append("⚠️ HALLUCINATION DETECTED: High confidence hours but no sign location described or explicitly states hours not visible. Likely fabricated.")
                    summary_reasons.append("Hours likely hallucinated - no visible sign")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(0.1)  # Very low confidence for hallucination
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                # RELAXED: Only block if SEVERE quality issues
                if has_severe_quality_issues:
                    recommendations.append("No change")
                    reasons.append("Severe image quality issues - hours completely unreadable")
                    summary_reasons.append("Hours completely unreadable")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue

                # NEW: Validate we have BOTH opening AND closing times
                incomplete_days = []
                for day, times in posted.items():
                    start = times.get("start", "")
                    end = times.get("end", "")
                    
                    # Check if we have BOTH start and end, and both are valid times
                    if not (start and end and re.match(r"^\d{2}:\d{2}:\d{2}$", start) and re.match(r"^\d{2}:\d{2}:\d{2}$", end)):
                        incomplete_days.append(day)
                
                # If we have ANY incomplete days in what was extracted, this is suspicious
                if incomplete_days and len(incomplete_days) == len(posted):
                    # ALL days are incomplete (e.g., only closing times visible)
                    recommendations.append("No change")
                    reasons.append(f"Incomplete hours detected - can only see opening OR closing times, not both. Cannot safely update hours with partial data.")
                    summary_reasons.append("Partial hours visible - need complete times")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity * 0.5)  # Lower confidence for partial data
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue

                listed = extract_hours(store_hours)

                if len(posted) in [1, 2]:
                    starts = set(v["start"] for v in posted.values() if v.get("start"))
                    ends = set(v["end"] for v in posted.values() if v.get("end"))
                    if len(starts) == 1 and len(ends) == 1:
                        same_start = list(starts)[0]
                        same_end = list(ends)[0]
                        posted = {day: {"start": same_start, "end": same_end} for day in bulk_hours}
                        parse_coverage = confidence_from_hours(posted)

                # RELAXED: Reduce from 5 days to 4 days minimum
                if len(posted) < 4:
                    recommendations.append("No change")
                    reasons.append("Too few days extracted to safely change hours (>=4 required)")
                    summary_reasons.append("Too few days extracted to safely change hours")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(hour_change_confidence(parse_coverage, clarity))
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue

                # Count how many days have significant differences (>30 min)
                diff_days = 0
                for day in posted:
                    if day in listed:
                        p_start, p_end = posted[day]["start"], posted[day]["end"]
                        l_start, l_end = listed[day]["start"], listed[day]["end"]
                        # Check if difference > 30 minutes (relaxed from 5 to handle minor OCR errors)
                        if time_diff_min(p_start, l_start) > 30 or time_diff_min(p_end, l_end) > 30:
                            diff_days += 1
                    else:
                        diff_days += 1

                # Decide whether to flag based on days that differ AND confidence
                if diff_days == 0:
                    # All days match - hours are correct
                    recommendations.append("No change")
                    reasons.append("Posted hours match current DoorDash hours - no change needed")
                    summary_reasons.append("Hours match current store hours")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(hour_change_confidence(parse_coverage, clarity))
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                elif diff_days == 1:
                    # Only 1 day differs - decide based on confidence
                    overall_confidence = hour_change_confidence(parse_coverage, clarity)
                    
                    if overall_confidence < 0.85:
                        # Low confidence - likely OCR error
                        recommendations.append("No change")
                        reasons.append("Only 1 day differs with low confidence ({:.2f}) - likely OCR error, not flagging".format(overall_confidence))
                        summary_reasons.append("Only minor/single-day time difference (low confidence)")
                        deactivation_reason_id.append("")
                        is_temp_deactivation.append(False)
                        confidence_scores.append(overall_confidence)
                        new_addresses.append("")
                        temp_duration.append("")
                        for day in bulk_hours:
                            bulk_hours[day]["start"].append("")
                            bulk_hours[day]["end"].append("")
                        continue
                    else:
                        # High confidence - flag for change even with 1 day difference
                        recommendations.append("Change Store Hours")
                        reasons.append(reason)
                        summary_reasons.append("Posted hours differ from DoorDash hours (high confidence single-day change)")
                        deactivation_reason_id.append("")
                        is_temp_deactivation.append(False)
                        confidence_scores.append(overall_confidence)
                        new_addresses.append("")
                        temp_duration.append("")
                        for day in bulk_hours:
                            raw_start = posted.get(day, {}).get("start", "")
                            raw_end = posted.get(day, {}).get("end", "")
                            bulk_hours[day]["start"].append(normalize_time(raw_start))
                            bulk_hours[day]["end"].append(normalize_time(raw_end))
                        continue
                else:
                    # 2+ days differ - recommend change
                    recommendations.append("Change Store Hours")
                    reasons.append(reason)
                    summary_reasons.append("Posted hours differ from DoorDash hours")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(hour_change_confidence(parse_coverage, clarity))
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        raw_start = posted.get(day, {}).get("start", "")
                        raw_end = posted.get(day, {}).get("end", "")
                        bulk_hours[day]["start"].append(normalize_time(raw_start))
                        bulk_hours[day]["end"].append(normalize_time(raw_end))
                    continue

            # If we get here, check if GPT explicitly recommended "No Change"
            if "recommendation: **no change**" in lower or "recommendation: no change" in lower:
                recommendations.append("No change")
                reasons.append(reason)
                summary_reasons.append("No change required (special hours detected)")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(clarity)
                new_addresses.append("")
                temp_duration.append("")
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue
            
            # If we get here, no recommendation was made by GPT
            recommendations.append("No change")
            reasons.append(reason)
            summary_reasons.append("No change required")
            deactivation_reason_id.append("")
            is_temp_deactivation.append(False)
            confidence_scores.append(clarity)
            new_addresses.append("")
            temp_duration.append("")
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")

        except Exception as e:
            recommendations.append("Error")
            reasons.append(str(e))
            summary_reasons.append("GPT error")
            deactivation_reason_id.append("")
            is_temp_deactivation.append(False)
            confidence_scores.append(0.0)
            new_addresses.append("")
            temp_duration.append("")
            special_hours_list.append([])  # NEW
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
    
    df["RECOMMENDATION"] = recommendations
    df["REASON"] = reasons
    df["SUMMARY_REASON"] = summary_reasons
    df["deactivation_reason_id"] = deactivation_reason_id
    df["is_temp_deactivation"] = is_temp_deactivation
    df["CONFIDENCE_SCORE"] = confidence_scores
    df["NEW_ADDRESS"] = new_addresses
    df["TEMP_DURATION"] = temp_duration
    df["SPECIAL_HOURS_RAW"] = special_hours_list  # NEW: Store raw special hours data

    for day in bulk_hours:
        df[f"start_time_{day}"] = bulk_hours[day]["start"]
        df[f"end_time_{day}"] = bulk_hours[day]["end"]
    
    return df

# ============= FUNCTION 3: CREATE BULK UPLOAD SHEETS =============
def create_bulk_upload_sheets(df):
    print("\n📋 Creating bulk upload sheets...")
    
    address_change_df = df[df['RECOMMENDATION'] == 'Address Change'].copy()
    perm_close_df = df[df['RECOMMENDATION'] == 'Permanently Close Store'].copy()
    temp_close_df = df[df['RECOMMENDATION'] == 'Temporarily Close For Day'].copy()
    change_hours_df = df[df['RECOMMENDATION'] == 'Change Store Hours'].copy()
    
    # Address change bulk upload
    if len(address_change_df) > 0:
        address_change_bulk = pd.DataFrame({
            'store_id': address_change_df['STORE_ID'].values,
            'new_address': address_change_df['NEW_ADDRESS'].values
        })
    else:
        address_change_bulk = pd.DataFrame(columns=['store_id', 'new_address'])
    
    if len(perm_close_df) > 0:
        perm_close_bulk = pd.DataFrame({
            'store_id': perm_close_df['STORE_ID'].values,
            'deactivation_reason_id': 23,
            'notes': 'DRSC AI tool flagged as perm closing'
        })
    else:
        perm_close_bulk = pd.DataFrame(columns=['store_id', 'deactivation_reason_id', 'notes'])
    
    # Temp close with dynamic duration (12 or 700)
    if len(temp_close_df) > 0:
        temp_close_bulk = pd.DataFrame({
            'store_id': temp_close_df['STORE_ID'].values,
            'deactivation_reason_id': 67,
            'is_temp_deactivation': 'TRUE',
            'duration': temp_close_df['TEMP_DURATION'].values,
            'notes': 'DRSC AI Tool marked as temp deactivate'
        })
    else:
        temp_close_bulk = pd.DataFrame(columns=['store_id', 'deactivation_reason_id', 'is_temp_deactivation', 'duration', 'notes'])
    
    if len(change_hours_df) > 0:
        change_hours_bulk = pd.DataFrame({
            'store_id': change_hours_df['STORE_ID'].values
        })
        
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        for day in days:
            start_col = f'start_time_{day}'
            end_col = f'end_time_{day}'
            change_hours_bulk[f'{day}_start_time'] = change_hours_df[start_col].values
            change_hours_bulk[f'{day}_end_time'] = change_hours_df[end_col].values
    else:
        cols = ['store_id']
        for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
            cols.extend([f'{day}_start_time', f'{day}_end_time'])
        change_hours_bulk = pd.DataFrame(columns=cols)
    
    # NEW: Create bulk_upload_special_hours sheet
    special_hours_records = []
    for idx, row in df.iterrows():
        store_id = row.get('STORE_ID', '')
        store_name = row.get('STORE_NAME', '')
        special_hours_raw = row.get('SPECIAL_HOURS_RAW', [])
        
        # Convert raw special hours to bulk upload format
        for special_hour in special_hours_raw:
            holiday_name = special_hour.get('holiday', '')
            is_open = special_hour.get('is_open', 'no')
            start_time = special_hour.get('start_time', '')
            end_time = special_hour.get('end_time', '')
            
            # Get the date for this holiday
            holiday_date = get_holiday_date(holiday_name)
            if holiday_date:
                date_str = holiday_date.strftime("%m/%d/%Y")
                
                # Format description
                description = f"{holiday_name} picked up by DRSC AI Tool"
                
                special_hours_records.append({
                    'date': date_str,
                    'store_id': store_id,
                    'store_name': store_name,
                    'open': is_open,
                    'start_time': start_time,
                    'end_time': end_time,
                    'description': description
                })
    
    if special_hours_records:
        bulk_upload_special_hours = pd.DataFrame(special_hours_records)
    else:
        bulk_upload_special_hours = pd.DataFrame(columns=['date', 'store_id', 'store_name', 'open', 'start_time', 'end_time', 'description'])
    
    print(f"✅ Created bulk upload sheets:")
    print(f"   - Address change: {len(address_change_bulk)} stores")
    print(f"   - Perm close: {len(perm_close_bulk)} stores")
    print(f"   - Temp close: {len(temp_close_bulk)} stores")
    print(f"   - Change hours: {len(change_hours_bulk)} stores")
    print(f"   - Special hours: {len(bulk_upload_special_hours)} records")  # NEW
    
    return address_change_bulk, perm_close_bulk, temp_close_bulk, change_hours_bulk, bulk_upload_special_hours

# ============= FUNCTION 4: SEND TO SLACK =============
def send_to_slack(df, timestamp_str):
    print("\n📤 Sending to Slack...")
    
    client = WebClient(token=SLACK_BOT_TOKEN)
    
    try:
        address_change_bulk, perm_close_bulk, temp_close_bulk, change_hours_bulk, bulk_upload_special_hours = create_bulk_upload_sheets(df)
        
        excel_filename = f'store_hours_analysis_{timestamp_str}.xlsx'
        
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Full_Analysis', index=False)
            address_change_bulk.to_excel(writer, sheet_name='Flag_New_Address', index=False)
            perm_close_bulk.to_excel(writer, sheet_name='Bulk_Upload_Perm_Close', index=False)
            temp_close_bulk.to_excel(writer, sheet_name='Bulk_Upload_Temp_Close', index=False)
            change_hours_bulk.to_excel(writer, sheet_name='Bulk_Upload_Change_Hours', index=False)
            bulk_upload_special_hours.to_excel(writer, sheet_name='Bulk_Upload_Special_Hours', index=False)  # NEW
        
        print(f"✅ Created Excel file: {excel_filename}")
        
        rec_counts = df['RECOMMENDATION'].value_counts().to_dict()
        
        summary_parts = []
        summary_parts.append("*Store Hours Analysis Complete*")
        summary_parts.append("")
        summary_parts.append(f"Total stores analyzed: {len(df)}")
        summary_parts.append(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary_parts.append("")
        summary_parts.append("Recommendations:")
        
        for rec, count in rec_counts.items():
            summary_parts.append(f"- {rec}: {count}")
        
        summary_parts.append("")
        summary_parts.append("Bulk Upload Sheets Ready:")
        summary_parts.append(f"- Flag_New_Address: {len(address_change_bulk)} stores")
        summary_parts.append(f"- Bulk_Upload_Perm_Close: {len(perm_close_bulk)} stores")
        summary_parts.append(f"- Bulk_Upload_Temp_Close: {len(temp_close_bulk)} stores")
        summary_parts.append(f"   (Duration: 12 for regular closures, 700 for 'until further notice')")
        summary_parts.append(f"- Bulk_Upload_Change_Hours: {len(change_hours_bulk)} stores")
        summary_parts.append(f"- Bulk_Upload_Special_Hours: {len(bulk_upload_special_hours)} records")  # NEW
        
        summary = "\n".join(summary_parts)
        
        print("📤 Uploading to Slack...")
        response = client.files_upload_v2(
            channel=SLACK_CHANNEL_ID,
            file=excel_filename,
            title=f"Store Hours Analysis - {datetime.datetime.now().strftime('%Y-%m-%d')}",
            initial_comment=summary
        )
        
        print(f"✅ Posted to #daily-ai-drsc-experiment")
        return response
        
    except SlackApiError as e:
        print(f"❌ Slack error: {e.response['error']}")
        raise

# ============= MAIN EXECUTION =============
if __name__ == "__main__":
    print("="*60)
    print("AUTOMATED STORE HOURS ANALYSIS")
    print("="*60 + "\n")
    
    try:
        df = get_mode_data()
        processed_df = process_store_hours(df)
        
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        csv_file = f'store_hours_analysis_{timestamp_str}.csv'
        processed_df.to_csv(csv_file, index=False)
        print(f"\n✅ Saved CSV backup to: {csv_file}")
        
        send_to_slack(processed_df, timestamp_str)
        
        print(f"\n📊 Summary:")
        print(f"   Total stores: {len(processed_df)}")
        print(f"   Recommendations:")
        for rec, count in processed_df['RECOMMENDATION'].value_counts().items():
            print(f"      - {rec}: {count}")
        
        print("\n✅ AUTOMATION COMPLETE!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
