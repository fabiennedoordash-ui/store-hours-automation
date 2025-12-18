# ============= HOLIDAY HOURS TREND ANALYZER - 2025 SEASON =============
import requests
import pandas as pd
import time
from io import StringIO
import openai
from tqdm import tqdm
import re
import datetime
from collections import defaultdict
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os

print("=" * 60)
print("HOLIDAY HOURS TREND ANALYZER - 2025 SEASON")
print("=" * 60)

# ============= CREDENTIALS =============
MODE_TOKEN = os.environ.get('MODE_TOKEN')
MODE_SECRET = os.environ.get('MODE_SECRET')
MODE_ACCOUNT = 'doordash'
REPORT_ID = 'b04acfd4da8b'  # Your new report ID
QUERY_ID = 'f0532f84ed46'   # Your new query ID

openai.api_key = os.environ.get('OPENAI_API_KEY')

SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
SLACK_CHANNEL_ID = 'C098G9URHEV'  # Your Slack channel

print(f"✅ Loaded environment variables")
print(f"   MODE_TOKEN: {'Set' if MODE_TOKEN else 'MISSING'}")
print(f"   MODE_SECRET: {'Set' if MODE_SECRET else 'MISSING'}")
print(f"   OPENAI_API_KEY: {'Set' if openai.api_key else 'MISSING'}")
print(f"   SLACK_BOT_TOKEN: {'Set' if SLACK_BOT_TOKEN else 'MISSING'}")

# ============= HOLIDAY CONFIGURATION FOR 2025/2026 =============
def get_holiday_config(year=None):
    """
    Get holiday dates and monitoring windows.
    Each holiday has a monitoring window that starts a few days before.
    """
    if year is None:
        year = datetime.date.today().year
    
    return {
        'Christmas Eve': {
            'date': datetime.date(year, 12, 24),
            'monitor_start': datetime.date(year, 12, 20),  # Start monitoring 4 days before
            'emoji': '🎄'
        },
        'Christmas Day': {
            'date': datetime.date(year, 12, 25),
            'monitor_start': datetime.date(year, 12, 20),
            'emoji': '🎄'
        },
        "New Year's Eve": {
            'date': datetime.date(year, 12, 31),
            'monitor_start': datetime.date(year, 12, 27),  # Start monitoring 4 days before
            'emoji': '🎉'
        },
        "New Year's Day": {
            'date': datetime.date(year + 1, 1, 1),
            'monitor_start': datetime.date(year, 12, 27),
            'emoji': '🎉'
        },
    }

def get_active_holidays(check_date=None):
    """
    Return list of holidays we should be monitoring for on a given date.
    Returns holidays where check_date falls within their monitoring window.
    """
    if check_date is None:
        check_date = datetime.date.today()
    
    # Check current year and handle year boundary
    configs_to_check = [get_holiday_config(check_date.year)]
    if check_date.month == 1:
        # In January, also check previous year's config for NYE/NYD
        configs_to_check.append(get_holiday_config(check_date.year - 1))
    
    active = []
    for config in configs_to_check:
        for holiday_name, holiday_info in config.items():
            monitor_start = holiday_info['monitor_start']
            holiday_date = holiday_info['date']
            
            # Active if we're between monitor_start and holiday_date (inclusive)
            if monitor_start <= check_date <= holiday_date:
                active.append({
                    'name': holiday_name,
                    'date': holiday_date,
                    'emoji': holiday_info['emoji'],
                    'days_until': (holiday_date - check_date).days
                })
    
    return active

def is_monitoring_period(check_date=None):
    """Check if we're in any holiday monitoring period."""
    return len(get_active_holidays(check_date)) > 0

def get_target_holidays_for_analysis(check_date=None):
    """Get the list of holiday names to look for in images based on current date."""
    active = get_active_holidays(check_date)
    return [h['name'] for h in active]

# ============= HELPER FUNCTIONS =============
def get_holiday_date(holiday_name, year=None):
    """Get the date for a given holiday"""
    if year is None:
        year = datetime.date.today().year
    
    config = get_holiday_config(year)
    if holiday_name in config:
        return config[holiday_name]['date']
    
    # Check next year for New Year's Day
    if holiday_name == "New Year's Day":
        next_config = get_holiday_config(year)
        if holiday_name in next_config:
            return next_config[holiday_name]['date']
    
    return None

def extract_holiday_hours(text, target_holidays):
    """Extract holiday hours with strict validation - only for target holidays"""
    holiday_hours = {}
    
    # Look for specific holiday mentions with hours
    for holiday in target_holidays:
        holiday_lower = holiday.lower()
        text_lower = text.lower()
        
        if holiday_lower in text_lower:
            # Look for patterns like "Christmas Eve: 8AM-6PM" or "Thanksgiving: CLOSED"
            pattern = rf"{holiday}[:\s]*([^\n]+)"
            match = re.search(pattern, text, re.IGNORECASE)
            
            if match:
                hours_text = match.group(1).strip()
                
                # Check if closed
                if any(word in hours_text.lower() for word in ['closed', 'close']):
                    holiday_hours[holiday] = 'CLOSED'
                # Check for regular/normal hours
                elif any(phrase in hours_text.lower() for phrase in ['regular hours', 'normal hours', 'standard hours', '24 hours']):
                    holiday_hours[holiday] = hours_text
                else:
                    # Try to extract hours
                    time_pattern = r'(\d{1,2}:?\d{0,2}\s*[ap]?m?)\s*[-–to]\s*(\d{1,2}:?\d{0,2}\s*[ap]?m?)'
                    time_match = re.search(time_pattern, hours_text, re.IGNORECASE)
                    if time_match:
                        start_time = time_match.group(1).strip()
                        end_time = time_match.group(2).strip()
                        # Clean up formatting
                        holiday_hours[holiday] = f"{start_time} - {end_time}".upper().replace('AM', ' AM').replace('PM', ' PM').strip()
    
    return holiday_hours

def extract_clarity_score(text):
    """Extract clarity score from GPT response"""
    m = re.search(r"clarity\s*score\s*[:\-]\s*(1(?:\.0+)?|0\.\d+|\.\d+)", text, re.IGNORECASE)
    if m:
        try:
            score = float(m.group(1))
            return round(score, 2)
        except:
            pass
    return 0.60

# ============= MAIN FUNCTIONS =============
def get_mode_data():
    """Fetch last 3 days of DRSC data from Mode"""
    print("\n🔄 Fetching last 3 days of data from Mode...")
    
    run_url = f'https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs'
    response = requests.post(run_url, auth=(MODE_TOKEN, MODE_SECRET))
    run_token = response.json()['token']
    print(f"✅ Run started: {run_token}")
    
    # Wait for query to complete
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
    
    # Get results
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
    
    print(f"✅ Retrieved {len(df)} store images from {df['BUSINESS_NAME'].nunique()} businesses\n")
    return df

def analyze_holiday_hours(df, target_holidays):
    """Analyze images for holiday hours only"""
    print("\n🤖 Analyzing images for holiday hours...")
    print(f"   Looking for: {', '.join(target_holidays)}")
    
    # Group by business to show progress
    businesses = df['BUSINESS_NAME'].unique()
    print(f"   Processing {len(businesses)} unique businesses...")
    
    results = []
    
    # Build dynamic holiday list for prompt
    holiday_list = "\n".join([f"- {h}" for h in target_holidays])
    
    for i, row in tqdm(df.iterrows(), total=len(df)):
        image_url = row.get("IMAGE_URL")
        image_confidence = row.get("IMAGE_CONFIDENCE", 0)
        
        # Skip low confidence images
        if not image_url or image_confidence < 0.5:
            continue
        
        # Dynamic prompt based on which holidays we're monitoring
        prompt = f"""
You are analyzing a store entrance photo to identify ONLY holiday hours announcements.

FOCUS: Look ONLY for signs about these specific holidays:
{holiday_list}

WHAT TO LOOK FOR:
1. Posted signs with holiday hours
2. Digital displays showing holiday schedules
3. Handwritten notices about holiday closures
4. Corporate holiday hour announcements

IF YOU FIND HOLIDAY HOURS:
List each holiday and its hours in this format:
[Holiday Name]: [Hours or CLOSED or Regular Hours]

Examples:
- Christmas Eve: 9:00 AM - 6:00 PM
- Christmas Day: CLOSED
- New Year's Eve: 10:00 AM - 8:00 PM
- New Year's Day: Regular Hours

IMPORTANT:
- ONLY report what you can actually see on signs
- DO NOT guess or infer typical holiday hours
- If no holiday hours are visible, say "NO HOLIDAY HOURS VISIBLE"
- Must have very clear visibility to report hours

At the end, provide:
Clarity score: X.XX (rating from 0.00 to 1.00)
"""

        try:
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]}
                ],
                max_tokens=500
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Extract clarity score
            clarity = extract_clarity_score(result_text)
            
            # Only process high-clarity results
            if clarity >= 0.90 and "NO HOLIDAY HOURS VISIBLE" not in result_text.upper():
                holiday_hours = extract_holiday_hours(result_text, target_holidays)
                
                if holiday_hours:
                    results.append({
                        'business_id': row.get('BUSINESS_ID', ''),
                        'business_name': row.get('BUSINESS_NAME', ''),
                        'cng_business_line': row.get('CNG_BUSINESS_LINE', ''),
                        'pick_model': row.get('PICK_MODEL', ''),
                        'store_id': row.get('STORE_ID', ''),
                        'image_url': image_url,
                        'report_date': row.get('CANCELLATION_DATE_UTC', ''),
                        'clarity_score': clarity,
                        'holiday_hours': holiday_hours,
                        'raw_response': result_text
                    })
            
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f"Error processing store {row.get('STORE_ID', 'unknown')}: {e}")
            continue
    
    print(f"✅ Found {len(results)} stores with holiday hours posted")
    return results

def aggregate_business_trends(results, target_holidays):
    """Aggregate holiday trends by business"""
    business_trends = defaultdict(lambda: defaultdict(list))
    business_metadata = {}
    
    for result in results:
        business_id = result['business_id']
        business_name = result['business_name']
        
        # Store business metadata
        if business_id not in business_metadata:
            business_metadata[business_id] = {
                'business_name': business_name,
                'cng_business_line': result['cng_business_line'],
                'pick_model': result['pick_model']
            }
        
        for holiday, hours in result['holiday_hours'].items():
            business_trends[business_id][holiday].append({
                'hours': hours,
                'store_id': result['store_id'],
                'image_url': result['image_url'],
                'clarity': result['clarity_score']
            })
    
    # Create summary with DATE column
    summary = []
    for business_id, holidays in business_trends.items():
        business_info = business_metadata[business_id]
        
        for holiday, stores_data in holidays.items():
            # Find most common pattern
            hours_patterns = [s['hours'] for s in stores_data]
            
            if len(hours_patterns) == 1:
                most_common = hours_patterns[0]
                pattern_count = 1
            else:
                most_common = max(set(hours_patterns), key=hours_patterns.count)
                pattern_count = hours_patterns.count(most_common)
            
            # Get the holiday date
            holiday_date = get_holiday_date(holiday)
            date_str = holiday_date.strftime("%m/%d/%Y") if holiday_date else ""
            
            summary.append({
                'Business ID': business_id,
                'Business Name': business_info['business_name'],
                'CNG Business Line': business_info['cng_business_line'],
                'Pick Model': business_info['pick_model'],
                'Holiday': holiday,
                'Date': date_str,
                'Stores Reporting': len(stores_data),
                'Most Common Pattern': most_common,
                'Pattern Frequency': f"{pattern_count}/{len(stores_data)} ({100*pattern_count/len(stores_data):.0f}%)",
                'Avg Clarity': f"{sum(s['clarity'] for s in stores_data)/len(stores_data):.2f}"
            })
    
    return summary, business_trends, business_metadata

def create_excel_output(summary, business_trends, business_metadata, results, target_holidays, active_holidays):
    """Create Excel file with trends and examples"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create descriptive filename based on which holidays we're monitoring
    holiday_short = "_".join([h['name'].replace("'", "").replace(" ", "") for h in active_holidays[:2]])
    filename = f'holiday_hours_{holiday_short}_{timestamp}.xlsx'
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Summary tab
        if len(summary) > 0:
            summary_df = pd.DataFrame(summary)
            summary_df = summary_df.sort_values(['Business Name', 'Holiday'])
            summary_df.to_excel(writer, sheet_name='Business_Trends', index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name='Business_Trends', index=False)
        
        # Examples tab - top 3 examples per business/holiday
        examples = []
        for business_id, holidays in business_trends.items():
            business_info = business_metadata[business_id]
            for holiday, stores_data in holidays.items():
                # Get top 3 highest clarity examples
                top_examples = sorted(stores_data, key=lambda x: x['clarity'], reverse=True)[:3]
                
                holiday_date = get_holiday_date(holiday)
                date_str = holiday_date.strftime("%m/%d/%Y") if holiday_date else ""
                
                for ex in top_examples:
                    examples.append({
                        'Business Name': business_info['business_name'],
                        'CNG Business Line': business_info['cng_business_line'],
                        'Pick Model': business_info['pick_model'],
                        'Holiday': holiday,
                        'Date': date_str,
                        'Store ID': ex['store_id'],
                        'Hours/Status': ex['hours'],
                        'Clarity Score': ex['clarity'],
                        'Image URL': ex['image_url']
                    })
        
        if len(examples) > 0:
            examples_df = pd.DataFrame(examples)
            examples_df.to_excel(writer, sheet_name='Examples_Evidence', index=False)
        else:
            pd.DataFrame().to_excel(writer, sheet_name='Examples_Evidence', index=False)
        
        # Raw data tab
        raw_data = []
        for result in results:
            raw_row = {
                'business_id': result['business_id'],
                'business_name': result['business_name'],
                'cng_business_line': result['cng_business_line'],
                'pick_model': result['pick_model'],
                'store_id': result['store_id'],
                'image_url': result['image_url'],
                'report_date': result['report_date'],
                'clarity_score': result['clarity_score']
            }
            # Add holiday columns (only for target holidays)
            for holiday in target_holidays:
                raw_row[holiday] = result['holiday_hours'].get(holiday, '')
            raw_data.append(raw_row)
        
        if len(raw_data) > 0:
            raw_df = pd.DataFrame(raw_data)
        else:
            raw_df = pd.DataFrame()
        raw_df.to_excel(writer, sheet_name='Raw_Data', index=False)
    
    print(f"✅ Created Excel file: {filename}")
    return filename

def send_to_slack(filename, summary_df, active_holidays):
    """Send results to Slack"""
    print("\n📤 Sending to Slack...")
    client = WebClient(token=SLACK_BOT_TOKEN)
    
    # Create summary message with dynamic holiday info
    total_businesses = summary_df['Business Name'].nunique() if len(summary_df) > 0 else 0
    total_detections = len(summary_df)
    
    # Build holiday countdown info
    holiday_status = []
    for h in active_holidays:
        if h['days_until'] == 0:
            holiday_status.append(f"{h['emoji']} *{h['name']}* is TODAY!")
        elif h['days_until'] == 1:
            holiday_status.append(f"{h['emoji']} *{h['name']}* is TOMORROW!")
        else:
            holiday_status.append(f"{h['emoji']} *{h['name']}* in {h['days_until']} days ({h['date'].strftime('%m/%d')})")
    
    holiday_countdown = "\n".join(holiday_status)
    target_holiday_names = [h['name'] for h in active_holidays]
    
    message = f"""
🎄 *Holiday Hours Trend Analysis Complete*

📅 *Monitoring for:*
{holiday_countdown}

📊 *Summary:*
• Analyzed 3 days of DRSC images
• Found holiday hours for {total_businesses} businesses
• {total_detections} business/holiday combinations detected

🏆 *Top Findings:*
"""
    
    # Add top patterns for each holiday being monitored
    for holiday in target_holiday_names:
        holiday_data = summary_df[summary_df['Holiday'] == holiday] if len(summary_df) > 0 else pd.DataFrame()
        if len(holiday_data) > 0:
            message += f"\n*{holiday}:*\n"
            # Get top 3 businesses for this holiday
            for _, row in holiday_data.head(3).iterrows():
                message += f"  • {row['Business Name']}: {row['Most Common Pattern']} ({row['Pattern Frequency']})\n"
    
    # Upload file
    try:
        response = client.files_upload_v2(
            channel=SLACK_CHANNEL_ID,
            file=filename,
            title=f"Holiday Hours Trends - {datetime.datetime.now().strftime('%Y-%m-%d')}",
            initial_comment=message
        )
        print("✅ Posted to Slack!")
        return response
    except SlackApiError as e:
        print(f"❌ Slack error: {e.response['error']}")
        raise

# ============= MAIN EXECUTION =============
if __name__ == "__main__":
    try:
        today = datetime.date.today()
        
        # Target holidays: Christmas Eve, Christmas Day, New Year's Eve, New Year's Day
        target_holidays = ['Christmas Eve', 'Christmas Day', "New Year's Eve", "New Year's Day"]
        
        # Build active_holidays list for display and Slack
        config = get_holiday_config(today.year)
        active_holidays = []
        for holiday_name in target_holidays:
            if holiday_name in config:
                h_info = config[holiday_name]
                days_until = (h_info['date'] - today).days
                active_holidays.append({
                    'name': holiday_name,
                    'date': h_info['date'],
                    'emoji': h_info['emoji'],
                    'days_until': days_until
                })
        
        print(f"\n🎄 Scanning for holiday hours...")
        print(f"   Looking for: {', '.join(target_holidays)}")
        for h in active_holidays:
            if h['days_until'] == 0:
                print(f"   {h['emoji']} {h['name']} is TODAY!")
            elif h['days_until'] == 1:
                print(f"   {h['emoji']} {h['name']} is TOMORROW!")
            elif h['days_until'] > 0:
                print(f"   {h['emoji']} {h['name']} in {h['days_until']} days ({h['date'].strftime('%m/%d')})")
            else:
                print(f"   {h['emoji']} {h['name']} was {abs(h['days_until'])} days ago")
        
        # Get data
        df = get_mode_data()
        
        # Analyze for holiday hours (only target holidays)
        results = analyze_holiday_hours(df, target_holidays)
        
        if len(results) > 0:
            # Aggregate trends
            summary, business_trends, business_metadata = aggregate_business_trends(results, target_holidays)
            
            # Create Excel output
            filename = create_excel_output(summary, business_trends, business_metadata, results, target_holidays, active_holidays)
            
            # Send to Slack
            summary_df = pd.DataFrame(summary) if summary else pd.DataFrame()
            send_to_slack(filename, summary_df, active_holidays)
            
            print(f"\n✅ Analysis complete!")
            print(f"   Found {len(results)} stores with holiday hours")
            if len(summary_df) > 0:
                print(f"   Top businesses:")
                for biz in summary_df['Business Name'].value_counts().head(5).index:
                    count = summary_df[summary_df['Business Name'] == biz]['Stores Reporting'].sum()
                    print(f"   - {biz}: {count} store detections")
        else:
            print("\n⚠️ No holiday hours detected in the last 3 days of images")
            print("   This could mean:")
            print("   - Stores haven't posted holiday hours yet")
            print("   - Images don't clearly show holiday signage")
            print("   - Holiday signs are not meeting the 90% clarity threshold")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
