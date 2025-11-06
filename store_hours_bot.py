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

# ============= HELPER FUNCTIONS =============
def categorize_closure(text):
    lower = text.lower()
    for category, terms in closure_categories.items():
        if any(t in lower for t in terms):
            return category
    return "other"

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
    m = re.search(r"clarity\s*score\s*[:\-]\s*(1(?:\.0+)?|0(?:\.\d+)?|\.\d+)", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except:
            pass
    if any(p in text.lower() for p in uncertain_phrases):
        return 0.2
    return 0.6

def combine_confidence(parse_coverage, clarity):
    return round(max(0.0, min(1.0, 0.6*parse_coverage + 0.4*clarity)), 2)

def is_permanent_closure(text):
    """Check if the text indicates a permanent closure"""
    lower = text.lower()
    
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
            "thank you for", "final", "last day", "we are closing",
            "location is closing", "this store is closing"
        ]
        if any(ctx in lower for ctx in permanent_context):
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
    
    bulk_hours = {day: {"start": [], "end": []} for day in [
        "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
    ]}
    
    # Additional quality check phrases for hour changes
    hour_quality_issues = [
        "glare", "reflection", "reflective", "glaring", 
        "distance", "background", "far away", "far from",
        "difficult to read", "hard to read", "hard to make out",
        "small text", "tiny", "difficult to see clearly",
        "behind glass", "through window"
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
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
            continue

        prompt = f"""
You are reviewing a Dasher photo of a store entrance. CRITICAL: Check for closure signs FIRST before trying to read store hours.

PRIORITY ORDER (check in this order):
1) Is there a PERMANENT closure sign? (e.g., "permanently closing", "closed permanently", "thank you for your support")
2) Is there a TEMPORARY closure sign? (e.g., "POWER OUT", "closed due to weather", "system down", "maintenance", "closed today")
3) Are the posted store hours clearly visible AND readable?

Choose ONE recommendation:
- **Permanently Close Store** - ONLY if you see clear permanent closure signage
- **Temporarily Close For Day** - If you see ANY temporary closure sign (power out, maintenance, weather, system issues, etc.) - DO NOT try to read hours if this applies
- **Change Store Hours** - ONLY if there are NO closure signs AND the hours are clearly readable (clarity > 0.9) AND not affected by glare/reflection/distance
- **No Change** - If hours are blurry/unreadable OR match DoorDash hours OR affected by glare/reflection

IMPORTANT: 
- If you see a "POWER OUT", "CLOSED", or any temporary closure sign, recommend "Temporarily Close For Day" and DO NOT attempt to extract store hours from the background.
- If the hours are visible but have ANY of these issues, recommend "No Change": glare, reflection, far away in background, behind glass with reflections, small/hard to read text
- Be honest if you're having ANY difficulty reading the exact times due to image quality issues like glare, reflection, distance, or if the hours are in the background behind glass

Current DoorDash hours: {store_hours}

If recommending changed hours, list the full weekly schedule clearly (e.g. Monday: 08:00 - 22:00).

Provide this line at the end:
Clarity score: X
Where X is a number between 0.0 and 1.0 for how clearly ANY signage is visible. If there's a closure sign, rate that sign's clarity. For hours, consider if glare/reflection/distance affects readability.
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

            # If clarity is too low, skip
            if clarity < 0.9:
                recommendations.append("No change")
                reasons.append("Clarity too low (<0.9), skipping recommendation")
                summary_reasons.append("Low clarity image")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(combine_confidence(parse_coverage, clarity))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # Check for uncertainty
            if any(p in lower for p in uncertain_phrases):
                recommendations.append("No change")
                reasons.append("Model expressed uncertainty despite clarity threshold")
                summary_reasons.append("Image unreadable or GPT uncertain")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(combine_confidence(parse_coverage, clarity))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # NEW: Check for image quality issues that affect hour readability
            has_quality_issues = any(phrase in lower for phrase in hour_quality_issues)

            # PRIORITY 1: Check for PERMANENT closure
            if "permanently close" in lower and is_permanent_closure(result):
                recommendations.append("Permanently Close Store")
                reasons.append(reason)
                summary_reasons.append("Permanent closure detected")
                deactivation_reason_id.append("23")
                is_temp_deactivation.append(False)
                confidence_scores.append(0.95)
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 2: Check for TEMPORARY closure (highest priority after permanent)
            if "temporarily close" in lower or "special hour" in lower:
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append(categorize_closure(lower))
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.8, combine_confidence(parse_coverage, clarity)))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # Additional temp closure checks
            if (any(phrase in lower for phrase in [
                "closed for the day", "closed today", "closed due to", "store is closed",
                "power out", "no power", "maintenance", "system down"
            ]) and "permanently" not in lower):
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append(categorize_closure(lower))
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.8, combine_confidence(parse_coverage, clarity)))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 3: Only NOW check for hour changes (if no closures detected)
            if "recommend" in lower and "change store hour" in lower:
                # NEW: Block hour changes if quality issues detected
                if has_quality_issues:
                    recommendations.append("No change")
                    reasons.append("Image quality issues detected (glare/reflection/distance) - skipping hour change")
                    summary_reasons.append("Image quality issues for hour extraction")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                if any(p in lower for p in uncertain_phrases):
                    recommendations.append("No change")
                    reasons.append("GPT suggested change but was uncertain")
                    summary_reasons.append("GPT suggested change but was uncertain")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
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

                if len(posted) < 5:
                    recommendations.append("No change")
                    reasons.append("Too few days extracted to safely change hours (>=5 required)")
                    summary_reasons.append("Too few days extracted to safely change hours")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue

                minor_diff = True
                for day in posted:
                    if day in listed:
                        p_start, p_end = posted[day]["start"], posted[day]["end"]
                        l_start, l_end = listed[day]["start"], listed[day]["end"]
                        if time_diff_min(p_start, l_start) > 5 or time_diff_min(p_end, l_end) > 5:
                            minor_diff = False
                            break
                    else:
                        minor_diff = False
                        break

                if minor_diff:
                    recommendations.append("No change")
                    reasons.append(reason)
                    summary_reasons.append("Only minor time difference")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
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
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        raw_start = posted.get(day, {}).get("start", "")
                        raw_end = posted.get(day, {}).get("end", "")
                        bulk_hours[day]["start"].append(normalize_time(raw_start))
                        bulk_hours[day]["end"].append(normalize_time(raw_end))
                    continue

            recommendations.append("No change")
            reasons.append(reason)
            summary_reasons.append("No change required")
            deactivation_reason_id.append("")
            is_temp_deactivation.append(False)
            confidence_scores.append(combine_confidence(parse_coverage, clarity))
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
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
    
    df["RECOMMENDATION"] = recommendations
    df["REASON"] = reasons
    df["SUMMARY_REASON"] = summary_reasons
    df["deactivation_reason_id"] = deactivation_reason_id
    df["is_temp_deactivation"] = is_temp_deactivation
    df["CONFIDENCE_SCORE"] = confidence_scores

    for day in bulk_hours:
        df[f"start_time_{day}"] = bulk_hours[day]["start"]
        df[f"end_time_{day}"] = bulk_hours[day]["end"]
    
    return df

# ============= FUNCTION 3: CREATE BULK UPLOAD SHEETS =============
def create_bulk_upload_sheets(df):
    print("\n📋 Creating bulk upload sheets...")
    
    perm_close_df = df[df['RECOMMENDATION'] == 'Permanently Close Store'].copy()
    temp_close_df = df[df['RECOMMENDATION'] == 'Temporarily Close For Day'].copy()
    change_hours_df = df[df['RECOMMENDATION'] == 'Change Store Hours'].copy()
    
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
            'duration': 12,
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
    print(f"   - Perm close: {len(perm_close_bulk)} stores")
    print(f"   - Temp close: {len(temp_close_bulk)} stores")
    print(f"   - Change hours: {len(change_hours_bulk)} stores")
    
    return perm_close_bulk, temp_close_bulk, change_hours_bulk

# ============= FUNCTION 4: SEND TO SLACK =============
def send_to_slack(df, timestamp_str):
    print("\n📤 Sending to Slack...")
    
    client = WebClient(token=SLACK_BOT_TOKEN)
    
    try:
        perm_close_bulk, temp_close_bulk, change_hours_bulk = create_bulk_upload_sheets(df)
        
        excel_filename = f'store_hours_analysis_{timestamp_str}.xlsx'
        
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Full_Analysis', index=False)
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
        summary_parts.append(f"- Bulk_Upload_Perm_Close: {len(perm_close_bulk)} stores")
        summary_parts.append(f"- Bulk_Upload_Temp_Close: {len(temp_close_bulk)} stores")
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
