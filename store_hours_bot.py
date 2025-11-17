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

# Payment system issues - MORE STRICT
payment_issue_phrases = [
    '"cash only"', "'cash only'", "sign says cash only",
    '"no credit"', "'no credit'", "credit cards not accepted",
    '"registers down"', "'registers down'", "register is down"
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

# ============= NEGATIVE CONTEXT DETECTION =============
def has_negative_context(text, phrase_position):
    """
    Check if a phrase at a given position has negative context before it.
    Returns True if the phrase is preceded by negative words.
    """
    context_start = max(0, phrase_position - 50)
    context_before = text[context_start:phrase_position].lower()
    
    negative_indicators = [
        "no ", "not ", "n't ", "there is no", "there are no", 
        "does not", "doesn't", "did not", "didn't", "without",
        "absence of", "lacking", "missing", "none", "neither",
        "there's no", "there isn't", "there aren't", "no sign",
        "no indication", "no evidence"
    ]
    
    return any(neg in context_before for neg in negative_indicators)

# ============= HOUR NORMALIZATION FUNCTIONS =============
def time_to_minutes(time_str):
    """Convert HH:MM or H:MM to minutes since midnight"""
    if not time_str or pd.isna(time_str):
        return None
    try:
        time_str = str(time_str).strip()
        if ':' in time_str:
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1][:2])
            return hours * 60 + minutes
    except:
        return None
    return None

def hours_are_identical(posted_hours_dict, doordash_hours_str):
    """
    Check if posted hours match DoorDash hours exactly
    Handles format conversions (8 a.m.-10 p.m. = 08:00-22:00)
    """
    try:
        # Parse DoorDash hours
        dd_hours = {}
        for day_entry in doordash_hours_str.split(', '):
            if ':' in day_entry:
                day, hours = day_entry.split(': ', 1)
                if ' - ' in hours:
                    start, end = hours.split(' - ')
                    dd_hours[day] = {'start': start, 'end': end}
        
        # Compare each day
        days_matched = 0
        days_different = 0
        
        for day, times in posted_hours_dict.items():
            if day in dd_hours:
                dd_times = dd_hours[day]
                
                # Convert to minutes for comparison
                posted_start_min = time_to_minutes(times.get('start', ''))
                posted_end_min = time_to_minutes(times.get('end', ''))
                dd_start_min = time_to_minutes(dd_times['start'])
                dd_end_min = time_to_minutes(dd_times['end'])
                
                if None in [posted_start_min, posted_end_min, dd_start_min, dd_end_min]:
                    continue
                
                # Check if they match (within 5 minute tolerance)
                if (abs(posted_start_min - dd_start_min) <= 5 and 
                    abs(posted_end_min - dd_end_min) <= 5):
                    days_matched += 1
                else:
                    days_different += 1
        
        # If all parsed days match, hours are identical
        return days_different == 0 and days_matched >= 4
        
    except Exception as e:
        print(f"Error comparing hours: {e}")
        return False

def detect_glass_reflection_cases(text, clarity_score):
    """
    Special handling for cases where hours are clearly readable but 
    clarity is lowered due to glass/reflection/glare
    """
    lower = text.lower()
    
    # Check if glass/reflection is mentioned
    glass_indicators = [
        "glass", "reflection", "glare", "window", "door",
        "on the glass", "through glass", "on glass door", "visible through"
    ]
    
    has_glass = any(ind in lower for ind in glass_indicators)
    
    # Check if specific hours are clearly stated - ENHANCED PATTERNS
    hour_patterns = [
        r'\d{1,2}\s*am\s*[-–]\s*\d{1,2}\s*pm',  # 6am - 10pm, 8 am - 9 pm
        r'\d{1,2}am[-–]\d{1,2}pm',  # 6am-10pm, 8am-9pm
        r'open\s+every\s+day',  # "open every day"
        r'everyday',  # "everyday"
        r'\d{1,2}:\d{2}\s*[-–]\s*\d{1,2}:\d{2}',  # 06:00 - 22:00
        r'mon[.\s-]*sat[.\s:]*\d{1,2}',  # Mon.-Sat.: 8
        r'sun[.\s:]*\d{1,2}',  # Sun.: 8
        r'hours:\s*\d{1,2}',  # hours: 8
        r'hours\s*mon',  # hours monday
    ]
    
    has_clear_hours = any(re.search(pattern, lower) for pattern in hour_patterns)
    
    # Additional check: if GPT explicitly states the hours in the response
    explicit_hour_statements = [
        "sign shows", "sign reads", "sign says", "displays",
        "clearly shows", "clearly states", "states", "visible", "reads"
    ]
    has_explicit_statement = any(stmt in lower for stmt in explicit_hour_statements)
    
    # If hours are clearly stated but clarity is lowered due to glass, boost it
    if (has_glass or has_explicit_statement) and has_clear_hours and 0.5 <= clarity_score < 0.90:
        # This is likely a case where hours are readable but glass lowered clarity
        return True, min(0.90, clarity_score + 0.30)  # Boost clarity by 0.30
    
    # Special cases for common patterns
    if "open every day" in lower and ("am" in lower or "pm" in lower):
        return True, min(0.90, clarity_score + 0.30)
    
    if "store hours" in lower and ("am" in lower or "pm" in lower):
        return True, min(0.90, clarity_score + 0.30)
    
    return False, clarity_score

def detect_sign_size_issues(text, clarity_score):
    """
    IMPROVED sign size/visibility detection - less strict for clear signs
    Returns (has_issue, reason)
    """
    lower = text.lower()
    
    # Check for EXPLICIT mentions that sign is unreadable
    explicitly_unreadable = [
        "cannot read", "illegible", "unreadable", "too blurry",
        "cannot make out", "unable to read", "too small to read",
        "no store hours visible", "hours not visible"
    ]
    
    if any(issue in lower for issue in explicitly_unreadable):
        return True, "Sign explicitly stated as unreadable"
    
    # For digital/LED signs, be less strict
    digital_indicators = ["digital", "led", "electronic", "display", "screen", "monitor", "board"]
    is_digital = any(ind in lower for ind in digital_indicators)
    
    # For large signs mentioned explicitly, trust GPT
    large_sign_indicators = ["large sign", "prominent", "clearly visible", "easy to read", 
                             "clearly shows", "clearly displays", "clearly reads", "store hours sign"]
    is_large = any(ind in lower for ind in large_sign_indicators)
    
    # If it's digital or large, and clarity is high, trust it
    if (is_digital or is_large) and clarity_score >= 0.85:
        return False, None
    
    # Check for specific location description
    location_descriptors = [
        "on the door", "on the window", "posted on", "displayed on",
        "visible on", "taped to", "attached to", "hanging on", 
        "on the glass", "on the wall", "next to the entrance",
        "beside the door", "above the", "below the", "on a",
        "located on", "positioned on", "in the", "at the", "near the"
    ]
    
    has_location = any(desc in lower for desc in location_descriptors)
    
    # Check if hours are clearly stated in the response
    hours_clearly_stated = False
    hour_patterns = [
        r'\d{1,2}:\d{2}\s*[ap]m\s*-\s*\d{1,2}:\d{2}\s*[ap]m',  # 8:00am - 9:00pm
        r'\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}',  # 08:00 - 21:00
        r'\d{1,2}\s*am\s*[-–]\s*\d{1,2}\s*pm',  # 8am - 10pm
        r'open\s+\d{1,2}',  # open 8
        r'everyday', r'every day'  # everyday/every day
    ]
    
    for pattern in hour_patterns:
        if re.search(pattern, lower):
            hours_clearly_stated = True
            break
    
    # If hours are clearly stated with good clarity, don't require strict location
    if hours_clearly_stated and clarity_score >= 0.85:
        return False, None
    
    # For hours changes, only require location if clarity is lower
    if "change store hour" in lower and not has_location and clarity_score < 0.85:
        return True, "No specific sign location described with lower clarity"
    
    # Size issues - but only if explicitly mentioned
    size_issues = [
        "small sign", "tiny sign", "distant sign", "far away",
        "hard to make out", "difficult to see", "barely visible",
        "too small", "can't quite", "squinting"
    ]
    
    if any(issue in lower for issue in size_issues):
        return True, "Sign described as too small to read reliably"
    
    # Check for generic descriptions ONLY with low clarity
    if clarity_score < 0.80:
        if "yellow sign" in lower and "dollar general" in lower:
            # Dollar General specific check
            if not hours_clearly_stated:
                return True, "Generic Dollar General sign description without clear hours"
    
    # Estimation phrases mean GPT is guessing
    estimation_phrases = [
        "appears to be", "seems to say", "looks like",
        "probably says", "might be", "could be",
        "standard hours", "typical hours", "usual hours"
    ]
    
    if any(phrase in lower for phrase in estimation_phrases):
        return True, "GPT appears to be estimating rather than reading"
    
    return False, None

def validate_gpt_extraction(result, clarity_score, recommendation):
    """
    FIXED validation - properly handle GPT's recommendations
    """
    lower = result.lower()
    
    # Check if specific hours are mentioned
    specific_hours_mentioned = any([
        re.search(r'\d{1,2}:\d{2}', lower),  # Any time format
        re.search(r'\d{1,2}\s*am', lower),  # 8am, 8 am
        re.search(r'\d{1,2}\s*pm', lower),  # 9pm, 9 pm
        "am" in lower or "pm" in lower,
        "everyday" in lower or "every day" in lower
    ])
    
    # If GPT claims high clarity but uses uncertain language, override
    uncertain_terms = ["might be", "appears to", "seems to", "probably"]
    if clarity_score > 0.8 and any(term in lower for term in uncertain_terms):
        return "No change", 0.3, "Uncertain language despite high clarity claim"
    
    # FIXED: Don't override temp close recommendations if they're valid
    if "temporarily close" in recommendation.lower():
        # Check if there's actually evidence of closure
        closure_indicators = [
            "systems are down", "closed", "power out", "no power",
            "cash only", "registers down", "maintenance", "until further notice",
            "temporarily closed", "closed for", "sorry", "inconvenience"
        ]
        
        if any(indicator in lower for indicator in closure_indicators):
            # This is a valid temp closure - don't override!
            return recommendation, clarity_score, None
    
    # If specific hours are mentioned with high clarity for hours change, trust it
    if "change store hours" in recommendation.lower():
        if specific_hours_mentioned and clarity_score >= 0.85:
            # Don't require strict location for obvious cases
            return recommendation, clarity_score, None
        
        # Only require location for lower clarity cases
        location_phrases = ["on the door", "on the window", "posted on", "on the glass", 
                           "display", "sign", "board", "placard", "shows", "reads"]
        if not any(loc in lower for loc in location_phrases) and clarity_score < 0.85:
            return "No change", 0.2, "No sign description with lower clarity"
    
    # Check for contradictory recommendations
    if "no store hours visible" in lower and recommendation == "Change Store Hours":
        return "No change", 0.1, "Contradiction: claims no hours visible but recommends change"
    
    return recommendation, clarity_score, None

# ============= UPDATED HELPER FUNCTIONS =============
def categorize_closure(text):
    lower = text.lower()
    for category, terms in closure_categories.items():
        if any(t in lower for t in terms):
            return category
    return "other"

def is_permanent_closure(text):
    """Check if the text indicates a permanent closure"""
    lower = text.lower()
    
    # First check if it's a long-term temp closure (takes precedence)
    for phrase in long_term_closure_phrases:
        if phrase in lower:
            index = lower.find(phrase)
            if not has_negative_context(text, index):
                return False
    
    strong_indicators = [
        "permanently closing", "closed permanently", "permanent closure",
        "permanently closed", "closing permanently", "will be permanently closing",
        "this location is now permanently closed"
    ]
    
    for phrase in strong_indicators:
        if phrase in lower:
            index = 0
            while index < len(lower):
                index = lower.find(phrase, index)
                if index == -1:
                    break
                if not has_negative_context(text, index):
                    return True
                index += len(phrase)
    
    return False

def is_long_term_closure(text):
    """Check if the text indicates a long-term temporary closure"""
    lower = text.lower()
    
    for phrase in long_term_closure_phrases:
        if phrase in lower:
            index = 0
            while index < len(lower):
                index = lower.find(phrase, index)
                if index == -1:
                    break
                if not has_negative_context(text, index):
                    return True
                index += len(phrase)
    
    return False

def is_payment_issue(text):
    """Check if the text ACTUALLY mentions payment system issues - STRICTER"""
    lower = text.lower()
    
    # Must explicitly mention these issues with quotes or "sign says"
    explicit_payment_issues = [
        '"cash only"', "'cash only'", "sign says cash only",
        '"no credit"', "'no credit'", "credit cards not accepted",
        '"registers down"', "'registers down'", "register is down",
        '"no ebt"', "'no ebt'", "ebt down", "ebt not working",
        '"ebt down"', "'ebt down'", "sign says no ebt"
    ]
    
    # Check for quoted or explicitly mentioned payment issues
    for issue in explicit_payment_issues:
        if issue in lower:
            index = lower.find(issue)
            if not has_negative_context(text, index):
                return True
    
    # If GPT just mentions these without quotes or "sign says", be suspicious
    vague_mentions = ["payment issue", "ebt issue", "no ebt/ebt down"]
    if any(mention in lower for mention in vague_mentions):
        # Check if it's actually quoted
        has_quotes = '"' in lower or "'" in lower or "sign says" in lower or "sign indicates" in lower
        if not has_quotes:
            return False  # Likely GPT interpretation, not actual sign
    
    return False

def is_address_change(text):
    """Check if the text indicates an address change/relocation"""
    lower = text.lower()
    
    for phrase in address_change_phrases:
        if phrase in lower:
            index = 0
            while index < len(lower):
                index = lower.find(phrase, index)
                if index == -1:
                    break
                if not has_negative_context(text, index):
                    return True
                index += len(phrase)
    
    return False

def extract_new_address(text):
    """Extract new address from text if mentioned"""
    lower = text.lower()
    
    for phrase in address_change_phrases:
        if phrase in lower:
            index = 0
            while index < len(lower):
                index = lower.find(phrase, index)
                if index == -1:
                    break
                
                if not has_negative_context(text, index):
                    search_start = index
                    search_text = text[search_start:min(len(text), search_start + 200)]
                    
                    address_pattern = r"(?:new address:|new location:|moved to:|find us at:)?\s*(\d+\s+[A-Za-z0-9\s,\.]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct)[A-Za-z0-9\s,\.]*)"
                    match = re.search(address_pattern, search_text, re.IGNORECASE)
                    if match:
                        return match.group(1).strip()
                
                index += len(phrase)
    
    return ""

def get_gpt_recommendation(text):
    """Extract the explicit recommendation from GPT's response - IMPROVED"""
    lower = text.lower()
    
    patterns = [
        r"recommendation:\s*\*\*([^*]+)\*\*",
        r"recommendation:\s*([^\n]+)",
        r"recommend:\s*\*\*([^*]+)\*\*",
        r"recommend:\s*([^\n]+)",
        r"\*\*([^*]+)\*\*"  # Sometimes just in bold
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, lower)
        for match in matches:
            # Check if this is actually a recommendation
            if any(rec in match for rec in ["temporarily close", "permanently close", 
                                             "change store hours", "no change", 
                                             "address change", "long term"]):
                recommendation = match.strip()
                return recommendation, True
    
    return "", False

def should_trust_gpt_recommendation(gpt_recommendation, clarity_score):
    """Determine if we should trust GPT's explicit recommendation"""
    if not gpt_recommendation:
        return False
    
    # FIXED: Lower threshold for temp closures
    if "temporarily close" in gpt_recommendation:
        return clarity_score >= 0.70  # Lower threshold for temp closures
    
    if clarity_score < 0.70:
        return False
    
    gpt_to_action = {
        "no change": "No change",
        "change store hours": "Change Store Hours",
        "temporarily close for day": "Temporarily Close For Day",
        "temporarily close for day - long term": "Temporarily Close For Day",
        "permanently close store": "Permanently Close Store",
        "address change": "Address Change"
    }
    
    return any(key in gpt_recommendation for key in gpt_to_action)

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
            
            e = datetime.datetime.strptime(end_clean, "%I:%M%p").strftime("%H:%M:%S")
            s = datetime.datetime.strptime(start_clean, "%I:%M%p").strftime("%H:%M:%S")
            
            day_lower = day.lower()
            hours[day_lower] = {"start": s, "end": e}
        except:
            try:
                s = datetime.datetime.strptime(start.strip(), "%H:%M").strftime("%H:%M:%S")
                e = datetime.datetime.strptime(end.strip(), "%H:%M").strftime("%H:%M:%S")
                day_lower = day.lower()
                hours[day_lower] = {"start": s, "end": e}
            except:
                continue
    return hours

def extract_special_hours(text, clarity_score=None):
    """Extract special holiday hours from text - STRICT VERSION"""
    special_hours = []
    
    # Only extract if clarity is high enough
    if clarity_score and clarity_score < 0.90:
        return special_hours
    
    hallucination_indicators = [
        "typically", "usually", "assume", "likely", "probably",
        "most stores", "many stores", "generally", "common practice"
    ]
    
    sign_indicators = [
        "sign shows", "sign reads", "posted", "displayed",
        "visible on", "written on", "notice states"
    ]
    
    text_lower = text.lower()
    has_physical_sign = any(indicator in text_lower for indicator in sign_indicators)
    
    if not has_physical_sign:
        return special_hours
    
    special_section_match = re.search(
        r'SPECIAL\s+HOLIDAY\s+HOURS\s*:\s*(.*?)(?:\n\n|\Z)',
        text,
        re.IGNORECASE | re.DOTALL
    )
    
    if not special_section_match:
        return special_hours
    
    section_text = special_section_match.group(1)
    section_lower = section_text.lower()
    
    if any(indicator in section_lower for indicator in hallucination_indicators):
        return special_hours
    
    # Parse holidays - simplified for brevity
    holidays_to_check = [
        ('thanksgiving', ['thanksgiving']),
        ('christmas', ['christmas']),
        ('new year', ['new year'])
    ]
    
    lines = section_text.split('\n')
    for line in lines:
        if not line.strip() or ':' not in line:
            continue
        
        lower_line = line.lower()
        
        found_holiday = None
        for canonical_name, keywords in holidays_to_check:
            for keyword in keywords:
                if keyword in lower_line:
                    found_holiday = canonical_name
                    break
            if found_holiday:
                break
        
        if found_holiday:
            parts = line.split(':', 1)
            if len(parts) >= 2:
                status_part = parts[1].strip()
                if 'closed' in status_part.lower():
                    special_hours.append({
                        'holiday': found_holiday,
                        'is_open': 'no',
                        'start_time': '',
                        'end_time': ''
                    })
    
    return special_hours

def get_holiday_date(holiday_name, year=2025):
    """Get the date for a given holiday"""
    lower_holiday = holiday_name.lower()
    
    if "thanksgiving" in lower_holiday:
        november_thursdays = []
        for day in range(1, 31):
            try:
                if datetime.date(year, 11, day).weekday() == 3:
                    november_thursdays.append(day)
            except ValueError:
                break
        if len(november_thursdays) >= 4:
            return datetime.date(year, 11, november_thursdays[3])
    elif "christmas" in lower_holiday:
        return datetime.date(year, 12, 25)
    elif "new year" in lower_holiday:
        return datetime.date(year + 1, 1, 1)
    
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
    try:
        dt1 = datetime.datetime.strptime(t1, "%H:%M:%S")
        dt2 = datetime.datetime.strptime(t2, "%H:%M:%S")
        delta = abs((dt1 - dt2).total_seconds())
        return min(delta, 86400 - delta) / 60
    except:
        return 999

def confidence_from_hours(posted_hours_dict):
    valid_days = 0
    for v in posted_hours_dict.values():
        if v.get("start") and v.get("end") and re.match(r"^\d{2}:\d{2}:\d{2}$", v["start"]) and re.match(r"^\d{2}:\d{2}:\d{2}$", v["end"]):
            valid_days += 1
    return round(min(max(valid_days / 7.0, 0.0), 1.0), 2)

def extract_clarity_score(text):
    m = re.search(r"clarity\s*score\s*[:\-]\s*(1(?:\.0+)?|0\.\d+|\.\d+)", text, re.IGNORECASE)
    if m:
        try:
            score = float(m.group(1))
            return round(score, 2)
        except:
            pass
    if any(p in text.lower() for p in uncertain_phrases):
        return 0.20
    return 0.60

def hour_change_confidence(parse_coverage, clarity):
    """Confidence score specifically for hour changes"""
    return round(max(0.0, min(1.0, 0.6*parse_coverage + 0.4*clarity)), 2)

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
    
    # DEDUPLICATE
    original_count = len(df)
    if 'STORE_ID' in df.columns:
        timestamp_cols = [col for col in df.columns if 'TIME' in col.upper() or 'DATE' in col.upper() or 'CREATED' in col.upper()]
        if timestamp_cols:
            try:
                df = df.sort_values(timestamp_cols[0], ascending=False)
            except:
                pass
        
        df = df.drop_duplicates(subset='STORE_ID', keep='first')
        
        if len(df) < original_count:
            print(f"⚠️  Removed {original_count - len(df)} duplicate stores")
    
    print(f"✅ Retrieved {len(df)} unique stores\n")
    return df

# ============= FUNCTION 2: PROCESS WITH OPENAI (FIXED LENGTH ISSUE) =============
def process_store_hours(df):
    print("\n🤖 Processing with OpenAI vision API...")
    
    recommendations, reasons, summary_reasons = [], [], []
    deactivation_reason_id, is_temp_deactivation = [], []
    confidence_scores = []
    new_addresses = []
    temp_duration = []
    special_hours_list = []
    
    bulk_hours = {day: {"start": [], "end": []} for day in [
        "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
    ]}
    
    # Helper function to append default values
    def append_default_values():
        recommendations.append("No change")
        reasons.append("Processing error or skipped")
        summary_reasons.append("Processing error or skipped")
        deactivation_reason_id.append("")
        is_temp_deactivation.append(False)
        confidence_scores.append(0.0)
        new_addresses.append("")
        temp_duration.append("")
        special_hours_list.append([])
        for day in bulk_hours:
            bulk_hours[day]["start"].append("")
            bulk_hours[day]["end"].append("")
    
    for i, row in tqdm(df.iterrows(), total=len(df)):
        # Flag to track if we've processed this row
        row_processed = False
        
        try:
            image_url = row.get("IMAGE_URL")
            store_hours = str(row.get("STORE_HOURS", ""))

            if not image_url or not store_hours:
                append_default_values()
                continue

            prompt = f"""
You are reviewing a Dasher photo of a store entrance. 

SIGN TYPES TO LOOK FOR:
- Digital/LED displays showing store hours
- Large printed signs or boards
- Window decals or stickers
- Posted paper signs
- Signs visible through glass doors/windows
- Any clearly visible hours display

IMPORTANT ABOUT GLASS/REFLECTIONS:
- If store hours are visible through glass with some reflection/glare but still READABLE, report them
- Glass doors often have reflections but if you can read the hours clearly, that's what matters
- State: "Hours visible on glass door with some reflection but readable" if applicable

For DIGITAL DISPLAYS, LARGE SIGNS, or SIGNS ON GLASS:
- These are typically readable even with some glare - report what you see
- Describe them accurately (e.g., "digital display", "large sign board", "sign on glass door")
- State the exact hours shown

Current DoorDash hours: {store_hours}

If the sign shows different hours than DoorDash (even by 1 hour), recommend "Change Store Hours".

PRIORITY ORDER (check in this order):
1) Is there a RELOCATION/ADDRESS CHANGE sign?
2) Is there a LONG-TERM TEMPORARY closure sign?
3) Is there a PERMANENT closure sign?
4) Is there a PAYMENT SYSTEM issue?
5) Is there a TEMPORARY closure sign?
6) Are the posted store hours clearly visible AND readable?

Choose ONE recommendation:
- **Address Change** - ONLY if THIS STORE is explicitly moving to a new address
- **Temporarily Close For Day - Long Term** - If you see "closed until further notice"
- **Permanently Close Store** - ONLY if you see clear permanent closure signage
- **Temporarily Close For Day** - For payment issues, power issues, temporary closure
- **Change Store Hours** - If hours are readable AND different from DoorDash
- **No Change** - If hours are unreadable or match DoorDash hours

CRITICAL RULES:
- If you can read the hours clearly (even through glass), report them
- For digital displays, large prominent signs, or glass door signs, always trust what you see
- State exactly what the sign says (e.g., "8:00am - 9:00pm Everyday")
- If sign is less than 10% of image and not digital/prominent, state "NO STORE HOURS VISIBLE - sign too small"
- Never use phrases like "appears to be", "seems to say", "probably says"

At the end, provide:
Clarity score: X.XX (0.00-1.00, two decimal places)
"""

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
            
            time.sleep(0.5)

            posted = extract_hours(result)
            parse_coverage = confidence_from_hours(posted)
            clarity = extract_clarity_score(result)
            
            # Get GPT's recommendation EARLY
            gpt_rec, found_rec = get_gpt_recommendation(result)
            
            # Check for glass/reflection cases where hours are still readable
            glass_case, adjusted_clarity = detect_glass_reflection_cases(result, clarity)
            if glass_case:
                clarity = adjusted_clarity
                if row.get('STORE_ID'):
                    print(f"   Store {row.get('STORE_ID')}: Adjusted clarity for glass/reflection from {extract_clarity_score(result):.2f} to {clarity:.2f}")

            # Check for sign size issues FIRST (IMPROVED VERSION)
            has_issue, issue_reason = detect_sign_size_issues(result, clarity)
            if has_issue:
                recommendations.append("No change")
                reasons.append(f"Sign validation failed: {issue_reason}")
                summary_reasons.append("Sign too small/unclear to read reliably")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(0.15)
                new_addresses.append("")
                temp_duration.append("")
                special_hours_list.append([])
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # Check if hours are actually identical to DoorDash
            if "change store hour" in lower and posted:
                if hours_are_identical(posted, store_hours):
                    recommendations.append("No change")
                    reasons.append("Hours match DoorDash hours - no change needed")
                    summary_reasons.append("Hours already correct")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    special_hours_list.append([])
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
            
            # Validate the extraction (FIXED VERSION)
            final_rec, final_clarity, validation_reason = validate_gpt_extraction(result, clarity, gpt_rec if found_rec else "")
            
            if validation_reason:
                recommendations.append("No change")
                reasons.append(validation_reason)
                summary_reasons.append(validation_reason)
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(final_clarity)
                new_addresses.append("")
                temp_duration.append("")
                special_hours_list.append([])
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # Extract special hours (only with high clarity)
            special_hours_extracted = extract_special_hours(result, clarity) if clarity >= 0.90 else []

            # Check for uncertainty
            if any(p in lower for p in uncertain_phrases):
                recommendations.append("No change")
                reasons.append("Model expressed uncertainty")
                summary_reasons.append("Image unreadable or GPT uncertain")
                deactivation_reason_id.append("")
                special_hours_list.append([])
                is_temp_deactivation.append(False)
                confidence_scores.append(0.20)
                new_addresses.append("")
                temp_duration.append("")
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # FIXED: Check if GPT explicitly recommended something valid
            if found_rec and should_trust_gpt_recommendation(gpt_rec, clarity):
                # Map GPT recommendation to action
                rec_mapping = {
                    "temporarily close for day": "Temporarily Close For Day",
                    "temporarily close for day - long term": "Temporarily Close For Day",
                    "permanently close store": "Permanently Close Store",
                    "change store hours": "Change Store Hours",
                    "address change": "Address Change",
                    "no change": "No change"
                }
                
                action_taken = False
                for key, action in rec_mapping.items():
                    if key in gpt_rec:
                        if action == "Temporarily Close For Day":
                            # Handle temp closures
                            is_long_term = "long term" in gpt_rec or is_long_term_closure(result)
                            recommendations.append("Temporarily Close For Day")
                            reasons.append(reason)
                            summary_reasons.append(categorize_closure(lower))
                            deactivation_reason_id.append("67")
                            special_hours_list.append(special_hours_extracted)
                            is_temp_deactivation.append(True)
                            confidence_scores.append(max(0.80, clarity))
                            new_addresses.append("")
                            temp_duration.append(700 if is_long_term else 12)
                            for day in bulk_hours:
                                bulk_hours[day]["start"].append("")
                                bulk_hours[day]["end"].append("")
                            action_taken = True
                            break
                        elif action == "Change Store Hours" and len(posted) >= 4:
                            recommendations.append("Change Store Hours")
                            reasons.append(reason)
                            summary_reasons.append("Posted hours differ from DoorDash")
                            deactivation_reason_id.append("")
                            special_hours_list.append(special_hours_extracted)
                            is_temp_deactivation.append(False)
                            confidence_scores.append(hour_change_confidence(parse_coverage, clarity))
                            new_addresses.append("")
                            temp_duration.append("")
                            for day in bulk_hours:
                                raw_start = posted.get(day, {}).get("start", "")
                                raw_end = posted.get(day, {}).get("end", "")
                                bulk_hours[day]["start"].append(normalize_time(raw_start))
                                bulk_hours[day]["end"].append(normalize_time(raw_end))
                            action_taken = True
                            break
                        elif action == "Permanently Close Store":
                            recommendations.append("Permanently Close Store")
                            reasons.append(reason)
                            summary_reasons.append("Permanent closure detected")
                            deactivation_reason_id.append("23")
                            special_hours_list.append(special_hours_extracted)
                            is_temp_deactivation.append(False)
                            confidence_scores.append(0.95)
                            new_addresses.append("")
                            temp_duration.append("")
                            for day in bulk_hours:
                                bulk_hours[day]["start"].append("")
                                bulk_hours[day]["end"].append("")
                            action_taken = True
                            break
                        elif action == "Address Change":
                            new_addr = extract_new_address(result)
                            recommendations.append("Address Change")
                            reasons.append(reason)
                            summary_reasons.append("Store relocation detected")
                            deactivation_reason_id.append("")
                            special_hours_list.append(special_hours_extracted)
                            is_temp_deactivation.append(False)
                            confidence_scores.append(max(0.85, clarity))
                            new_addresses.append(new_addr)
                            temp_duration.append("")
                            for day in bulk_hours:
                                bulk_hours[day]["start"].append("")
                                bulk_hours[day]["end"].append("")
                            action_taken = True
                            break
                        elif action == "No change":
                            recommendations.append("No change")
                            reasons.append(reason)
                            summary_reasons.append("No change required")
                            deactivation_reason_id.append("")
                            special_hours_list.append(special_hours_extracted)
                            is_temp_deactivation.append(False)
                            confidence_scores.append(clarity)
                            new_addresses.append("")
                            temp_duration.append("")
                            for day in bulk_hours:
                                bulk_hours[day]["start"].append("")
                                bulk_hours[day]["end"].append("")
                            action_taken = True
                            break
                
                if action_taken:
                    row_processed = True
                    continue
            
            # Process recommendations by priority (as fallback if GPT rec didn't work)
            
            # ADDRESS CHANGE (keeping strict 0.92 for address changes)
            if is_address_change(result):
                if clarity < 0.92:
                    recommendations.append("No change")
                    reasons.append(f"Clarity too low for address change ({clarity:.2f} < 0.92)")
                    summary_reasons.append("Clarity too low")
                    deactivation_reason_id.append("")
                    special_hours_list.append([])
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
                
                new_addr = extract_new_address(result)
                recommendations.append("Address Change")
                reasons.append(reason)
                summary_reasons.append("Store relocation detected")
                deactivation_reason_id.append("")
                special_hours_list.append(special_hours_extracted)
                is_temp_deactivation.append(False)
                confidence_scores.append(max(0.85, clarity))
                new_addresses.append(new_addr)
                temp_duration.append("")
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # LONG-TERM CLOSURE
            if is_long_term_closure(result):
                if clarity < 0.75:
                    recommendations.append("No change")
                    reasons.append(f"Clarity too low ({clarity:.2f} < 0.75)")
                    summary_reasons.append("Clarity too low")
                    deactivation_reason_id.append("")
                    special_hours_list.append([])
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
                
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append("Closed until further notice")
                deactivation_reason_id.append("67")
                special_hours_list.append(special_hours_extracted)
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.85, clarity))
                new_addresses.append("")
                temp_duration.append(700)
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # PERMANENT CLOSURE
            if "permanently close" in lower and is_permanent_closure(result):
                if clarity < 0.85:
                    recommendations.append("No change")
                    reasons.append(f"Clarity too low for permanent closure ({clarity:.2f} < 0.85)")
                    summary_reasons.append("Clarity too low")
                    deactivation_reason_id.append("")
                    special_hours_list.append([])
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
                
                recommendations.append("Permanently Close Store")
                reasons.append(reason)
                summary_reasons.append("Permanent closure detected")
                deactivation_reason_id.append("23")
                special_hours_list.append(special_hours_extracted)
                is_temp_deactivation.append(False)
                confidence_scores.append(0.95)
                new_addresses.append("")
                temp_duration.append("")
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # PAYMENT ISSUES (STRICTER)
            if is_payment_issue(result):
                if clarity < 0.75:
                    recommendations.append("No change")
                    reasons.append(f"Clarity too low ({clarity:.2f} < 0.75)")
                    summary_reasons.append("Clarity too low")
                    deactivation_reason_id.append("")
                    special_hours_list.append([])
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
                
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append("Payment issue")
                deactivation_reason_id.append("67")
                special_hours_list.append(special_hours_extracted)
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.80, clarity))
                new_addresses.append("")
                temp_duration.append(12)
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # TEMPORARY CLOSURE
            temp_closure_phrases = [
                "closed for the day", "closed today", "closed due to",
                "power out", "no power", "maintenance", "system down",
                "systems are down", "all systems are down", "sorry", "inconvenience"
            ]
            
            if any(phrase in lower for phrase in temp_closure_phrases):
                if clarity < 0.75:
                    recommendations.append("No change")
                    reasons.append(f"Clarity too low ({clarity:.2f} < 0.75)")
                    summary_reasons.append("Clarity too low")
                    deactivation_reason_id.append("")
                    special_hours_list.append([])
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
                
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append(categorize_closure(lower))
                deactivation_reason_id.append("67")
                special_hours_list.append(special_hours_extracted)
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.80, clarity))
                new_addresses.append("")
                temp_duration.append(12)
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                row_processed = True
                continue

            # HOUR CHANGES (Using 0.90 clarity as requested)
            if "change store hour" in lower or final_rec == "Change Store Hours":
                # Using 0.90 clarity for hour changes as requested
                if clarity < 0.90:
                    recommendations.append("No change")
                    reasons.append(f"Clarity too low for hour changes ({clarity:.2f} < 0.90)")
                    summary_reasons.append("Clarity too low for hour changes")
                    deactivation_reason_id.append("")
                    special_hours_list.append([])
                    is_temp_deactivation.append(False)
                    confidence_scores.append(clarity)
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
                
                # Need at least 4 days extracted
                if len(posted) < 4:
                    recommendations.append("No change")
                    reasons.append("Too few days extracted (need >=4)")
                    summary_reasons.append("Insufficient days extracted")
                    deactivation_reason_id.append("")
                    special_hours_list.append([])
                    is_temp_deactivation.append(False)
                    confidence_scores.append(hour_change_confidence(parse_coverage, clarity))
                    new_addresses.append("")
                    temp_duration.append("")
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    row_processed = True
                    continue
                
                recommendations.append("Change Store Hours")
                reasons.append(reason)
                summary_reasons.append("Posted hours differ from DoorDash")
                deactivation_reason_id.append("")
                special_hours_list.append(special_hours_extracted)
                is_temp_deactivation.append(False)
                confidence_scores.append(hour_change_confidence(parse_coverage, clarity))
                new_addresses.append("")
                temp_duration.append("")
                for day in bulk_hours:
                    raw_start = posted.get(day, {}).get("start", "")
                    raw_end = posted.get(day, {}).get("end", "")
                    bulk_hours[day]["start"].append(normalize_time(raw_start))
                    bulk_hours[day]["end"].append(normalize_time(raw_end))
                row_processed = True
                continue

            # Default: No change
            recommendations.append("No change")
            reasons.append(reason)
            summary_reasons.append("No change required")
            deactivation_reason_id.append("")
            special_hours_list.append(special_hours_extracted)
            is_temp_deactivation.append(False)
            confidence_scores.append(clarity)
            new_addresses.append("")
            temp_duration.append("")
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
            row_processed = True

        except Exception as e:
            error_msg = str(e)
            print(f"⚠️ Row {i}: {error_msg[:100]}")
            import traceback
            traceback.print_exc()
            
            # If we haven't processed this row yet, add default values
            if not row_processed:
                recommendations.append("Error")
                reasons.append(f"Exception: {error_msg[:200]}")
                summary_reasons.append("Processing error")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(0.0)
                new_addresses.append("")
                temp_duration.append("")
                special_hours_list.append([])
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
    
    # Verify all lists have the same length
    expected_length = len(df)
    assert len(recommendations) == expected_length, f"recommendations has {len(recommendations)} items, expected {expected_length}"
    assert len(reasons) == expected_length, f"reasons has {len(reasons)} items, expected {expected_length}"
    assert len(summary_reasons) == expected_length, f"summary_reasons has {len(summary_reasons)} items, expected {expected_length}"
    assert len(deactivation_reason_id) == expected_length, f"deactivation_reason_id has {len(deactivation_reason_id)} items, expected {expected_length}"
    assert len(is_temp_deactivation) == expected_length, f"is_temp_deactivation has {len(is_temp_deactivation)} items, expected {expected_length}"
    assert len(confidence_scores) == expected_length, f"confidence_scores has {len(confidence_scores)} items, expected {expected_length}"
    assert len(new_addresses) == expected_length, f"new_addresses has {len(new_addresses)} items, expected {expected_length}"
    assert len(temp_duration) == expected_length, f"temp_duration has {len(temp_duration)} items, expected {expected_length}"
    assert len(special_hours_list) == expected_length, f"special_hours_list has {len(special_hours_list)} items, expected {expected_length}"
    
    # Assign results
    df["RECOMMENDATION"] = recommendations
    df["REASON"] = reasons
    df["SUMMARY_REASON"] = summary_reasons
    df["deactivation_reason_id"] = deactivation_reason_id
    df["is_temp_deactivation"] = is_temp_deactivation
    df["CONFIDENCE_SCORE"] = confidence_scores
    df["NEW_ADDRESS"] = new_addresses
    df["TEMP_DURATION"] = temp_duration
    df["SPECIAL_HOURS_RAW"] = special_hours_list

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
            
            start_times = change_hours_df[start_col].values
            end_times = change_hours_df[end_col].values
            
            processed_start_times = []
            processed_end_times = []
            processed_start_times_2 = []
            processed_end_times_2 = []
            
            for start, end in zip(start_times, end_times):
                if start and end:
                    try:
                        start_hour = int(start.split(':')[0])
                        end_hour = int(end.split(':')[0])
                        
                        if start_hour > end_hour:
                            processed_start_times.append(start)
                            processed_end_times.append('23:59:59')
                            processed_start_times_2.append('00:00:00')
                            processed_end_times_2.append(end)
                        else:
                            processed_start_times.append(start)
                            processed_end_times.append(end)
                            processed_start_times_2.append('')
                            processed_end_times_2.append('')
                    except:
                        processed_start_times.append(start)
                        processed_end_times.append(end)
                        processed_start_times_2.append('')
                        processed_end_times_2.append('')
                else:
                    processed_start_times.append('')
                    processed_end_times.append('')
                    processed_start_times_2.append('')
                    processed_end_times_2.append('')
            
            change_hours_bulk[f'{day}_start_time'] = processed_start_times
            change_hours_bulk[f'{day}_end_time'] = processed_end_times
            change_hours_bulk[f'{day}_start_time_2'] = processed_start_times_2
            change_hours_bulk[f'{day}_end_time_2'] = processed_end_times_2
    else:
        cols = ['store_id']
        for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
            cols.extend([f'{day}_start_time', f'{day}_end_time', f'{day}_start_time_2', f'{day}_end_time_2'])
        change_hours_bulk = pd.DataFrame(columns=cols)
    
    # Special hours
    special_hours_records = []
    for idx, row in df.iterrows():
        store_id = row.get('STORE_ID', '')
        store_name = row.get('STORE_NAME', '')
        special_hours_raw = row.get('SPECIAL_HOURS_RAW', [])
        
        for special_hour in special_hours_raw:
            holiday_name = special_hour.get('holiday', '')
            is_open = special_hour.get('is_open', 'no')
            start_time = special_hour.get('start_time', '')
            end_time = special_hour.get('end_time', '')
            
            holiday_date = get_holiday_date(holiday_name)
            if holiday_date:
                date_str = holiday_date.strftime("%m/%d/%Y")
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
    print(f"   - Special hours: {len(bulk_upload_special_hours)} records")
    
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
            bulk_upload_special_hours.to_excel(writer, sheet_name='Bulk_Upload_Special_Hours', index=False)
        
        print(f"✅ Created Excel file: {excel_filename}")
        
        rec_counts = df['RECOMMENDATION'].value_counts().to_dict()
        total_stores = len(df)
        
        summary_parts = []
        summary_parts.append("DRSC AI Stats:")
        summary_parts.append("Store Hours Analysis Complete")
        summary_parts.append(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary_parts.append("")
        
        summary_parts.append(f"• *Total stores analyzed: {total_stores}*")
        
        no_change_count = rec_counts.get('No change', 0)
        no_change_pct = (no_change_count / total_stores * 100) if total_stores > 0 else 0
        summary_parts.append(f"• *No Change*: {no_change_count} stores, {no_change_pct:.1f}% of total stores")
        
        change_hours_count = len(change_hours_bulk)
        change_hours_pct = (change_hours_count / total_stores * 100) if total_stores > 0 else 0
        summary_parts.append(f"• *Change Hours*: {change_hours_count} stores in Bulk_Upload_Change_Hours, {change_hours_pct:.1f}% of total stores")
        
        temp_close_count = len(temp_close_bulk)
        temp_close_pct = (temp_close_count / total_stores * 100) if total_stores > 0 else 0
        summary_parts.append(f"• *Temp Close*: {temp_close_count} stores in Bulk_Upload_Temp_Close, {temp_close_pct:.1f}% of total stores")
        
        perm_close_count = len(perm_close_bulk)
        perm_close_pct = (perm_close_count / total_stores * 100) if total_stores > 0 else 0
        summary_parts.append(f"• *Perm Close*: {perm_close_count} stores in Bulk_Upload_Perm_Close, {perm_close_pct:.1f}% of total stores")
        
        address_change_count = len(address_change_bulk)
        address_change_pct = (address_change_count / total_stores * 100) if total_stores > 0 else 0
        summary_parts.append(f"• *Update address*: {address_change_count} stores in Flag_New_Address, {address_change_pct:.1f}% of total stores")
        
        special_hours_stores = bulk_upload_special_hours['store_id'].nunique() if len(bulk_upload_special_hours) > 0 else 0
        special_hours_pct = (special_hours_stores / total_stores * 100) if total_stores > 0 else 0
        summary_parts.append(f"• *Special Hours*: {special_hours_stores} stores in Bulk_Upload_Special_Hours, {special_hours_pct:.1f}% of total stores")
        
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
    print("AUTOMATED STORE HOURS ANALYSIS - FIXED VERSION V2")
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
