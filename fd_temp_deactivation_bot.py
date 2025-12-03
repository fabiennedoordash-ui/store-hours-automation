# ============= IMPORTS =============
import requests
import pandas as pd
import time
from io import StringIO
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from datetime import datetime

# ============= CREDENTIALS (from environment variables) =============
MODE_TOKEN = os.environ.get('MODE_TOKEN')
MODE_SECRET = os.environ.get('MODE_SECRET')
MODE_ACCOUNT = 'doordash'
REPORT_ID = 'e908b96aa50a'
QUERY_ID = 'ed50b2101c26'

SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
SLACK_CHANNEL_ID = 'C098G9URHEV'  # #daily-ai-drsc-experiment - update if different

# User IDs for @mentions (update these with actual Slack user IDs)
# To find user IDs: In Slack, click on user profile > More > Copy member ID
RACHEL_USER_ID = 'UXXXXXXXXXX'  # Replace with Rachel Weinbren's Slack user ID
SHAAN_USER_ID = 'UXXXXXXXXXX'   # Replace with Shaan's Slack user ID
FABIENNE_USER_ID = 'UXXXXXXXXXX'  # Replace with Fabienne's Slack user ID


# ============= MODE API FUNCTIONS =============
def run_mode_report():
    """Trigger a Mode report run and return the run token."""
    url = f"https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs"
    response = requests.post(url, auth=(MODE_TOKEN, MODE_SECRET))
    response.raise_for_status()
    return response.json()['token']


def wait_for_report(run_token, max_wait=300):
    """Wait for the Mode report to complete."""
    url = f"https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs/{run_token}"
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        response = requests.get(url, auth=(MODE_TOKEN, MODE_SECRET))
        response.raise_for_status()
        state = response.json()['state']
        
        if state == 'succeeded':
            return True
        elif state in ['failed', 'cancelled']:
            print(f"Report run failed with state: {state}")
            return False
        
        print(f"Report state: {state}. Waiting...")
        time.sleep(10)
    
    print("Timeout waiting for report")
    return False


def get_query_results(run_token):
    """Fetch CSV results from the completed Mode query."""
    url = f"https://app.mode.com/api/{MODE_ACCOUNT}/reports/{REPORT_ID}/runs/{run_token}/queries/{QUERY_ID}/results/content.csv"
    response = requests.get(url, auth=(MODE_TOKEN, MODE_SECRET))
    response.raise_for_status()
    return response.text


# ============= SLACK FUNCTIONS =============
def send_slack_message_with_csv(df):
    """Send the Slack message with CSV attachment."""
    client = WebClient(token=SLACK_BOT_TOKEN)
    
    # Create the message with bold first line and @mentions
    message = (
        f"*Family Dollar Temp Deactivated Stores (sent in by Mx)*\n\n"
        f"Hi Live Ops team, FD has sent the following stores to be deactivated. "
        f"Please upload the following output to the temp deactivation bulk tool: "
        f"https://admin-gateway.doordash.com/tools/bulk_tools/categories/store/temporary_deactivation\n\n"
        f"cc <@{RACHEL_USER_ID}> <@{SHAAN_USER_ID}> <@{FABIENNE_USER_ID}>"
    )
    
    # Save DataFrame to CSV
    today = datetime.now().strftime('%Y-%m-%d')
    csv_filename = f"fd_temp_deactivated_stores_{today}.csv"
    csv_content = df.to_csv(index=False)
    
    try:
        # Upload CSV file to Slack
        response = client.files_upload_v2(
            channel=SLACK_CHANNEL_ID,
            content=csv_content,
            filename=csv_filename,
            initial_comment=message,
            title=f"Family Dollar Temp Deactivated Stores - {today}"
        )
        print(f"✅ Successfully sent to Slack! File: {csv_filename}")
        print(f"   Rows sent: {len(df)}")
        return True
        
    except SlackApiError as e:
        print(f"❌ Slack API error: {e.response['error']}")
        return False


# ============= MAIN EXECUTION =============
def main():
    print("=" * 60)
    print(f"Family Dollar Temp Deactivation Bot - {datetime.now()}")
    print("=" * 60)
    
    # Step 1: Run Mode report
    print("\n📊 Running Mode report...")
    try:
        run_token = run_mode_report()
        print(f"   Run token: {run_token}")
    except Exception as e:
        print(f"❌ Failed to start Mode report: {e}")
        return
    
    # Step 2: Wait for completion
    print("\n⏳ Waiting for report to complete...")
    if not wait_for_report(run_token):
        print("❌ Report did not complete successfully")
        return
    
    # Step 3: Get results
    print("\n📥 Fetching query results...")
    try:
        csv_data = get_query_results(run_token)
        df = pd.read_csv(StringIO(csv_data))
        print(f"   Retrieved {len(df)} rows")
    except Exception as e:
        print(f"❌ Failed to fetch results: {e}")
        return
    
    # Step 4: Check if results are not empty
    if df.empty:
        print("\n✅ No stores to deactivate today. Skipping Slack notification.")
        return
    
    # Step 5: Send to Slack
    print(f"\n📤 Sending {len(df)} stores to Slack...")
    send_slack_message_with_csv(df)
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
