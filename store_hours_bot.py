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

def is_address_change(text):
    """Check if the text indicates an address change/relocation - VERY STRICT"""
    lower = text.lower()
    # Must have very explicit phrases
    return any(phrase in lower for phrase in address_change_phrases)

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
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
            continue

        prompt = f"""
You are reviewing a Dasher photo of a store entrance. CRITICAL: Check for closure and relocation signs FIRST before trying to read store hours.

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

Examples of what NOT to do:
❌ Sign shows "Closes at 9 PM" → DO NOT assume opening time → DO NOT recommend hour change
❌ Sign shows "Open 8 AM" → DO NOT assume closing time → DO NOT recommend hour change
✅ Sign shows "Monday: 8 AM - 9 PM" → Both times visible → Can recommend change

If recommending changed hours, list the full weekly schedule with BOTH opening and closing times clearly (e.g. Monday: 08:00 - 22:00).

If you detect an address change, include a line:
New Address: [the new address if visible, or "Not shown" if not visible]

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
            if "temporarily close" in lower or "special hour" in lower:
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

                # Only flag as "Change Store Hours" if at least 2 days differ
                if diff_days < 2:
                    recommendations.append("No change")
                    reasons.append("Only 1 day differs - likely OCR error, not flagging")
                    summary_reasons.append("Only minor/single-day time difference")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(hour_change_confidence(parse_coverage, clarity))
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                else:
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

            # If we get here, no recommendation was made
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
    
    print(f"✅ Created bulk upload sheets:")
    print(f"   - Address change: {len(address_change_bulk)} stores")
    print(f"   - Perm close: {len(perm_close_bulk)} stores")
    print(f"   - Temp close: {len(temp_close_bulk)} stores")
    print(f"   - Change hours: {len(change_hours_bulk)} stores")
    
    return address_change_bulk, perm_close_bulk, temp_close_bulk, change_hours_bulk

# ============= FUNCTION 4: SEND TO SLACK =============
def send_to_slack(df, timestamp_str):
    print("\n📤 Sending to Slack...")
    
    client = WebClient(token=SLACK_BOT_TOKEN)
    
    try:
        address_change_bulk, perm_close_bulk, temp_close_bulk, change_hours_bulk = create_bulk_upload_sheets(df)
        
        excel_filename = f'store_hours_analysis_{timestamp_str}.xlsx'
        
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Full_Analysis', index=False)
            address_change_bulk.to_excel(writer, sheet_name='Flag_New_Address', index=False)
            perm_close_bulk.to_excel(writer, sheet_name='Bulk_Upload_Perm_Close', index=False)
            temp_close_bulk.to_excel(writer, sheet_name='Bulk_Upload_Temp_Close', index=False)
            change_hours_bulk.to_excel(writer, sheet_name='Bulk_Upload_Change_Hours', index=False)
        
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
