

import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['signal_data_update dag'])
def signal_data_update_taskflow_api():

    @task()
    def signal_data_update():
        import psycopg2
        import requests
        import json
        import pandas as pd
        from urllib.parse import urlparse
        from datetime import datetime, timedelta

        # --- 1. PostgreSQL Connection ---
        db_url = 'postgresql://stpostgres:stocktrader@sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com:5432/postgres'
        result = urlparse(db_url)

        conn = psycopg2.connect(
            dbname=result.path[1:],  # remove leading /
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port
        )
        cur = conn.cursor()

        # --- 2. Fetch Leads (without signal data) ---
        cur.execute("""
            SELECT id, stock_name, lead_date
            FROM stocktrader.leads_1
            WHERE signal_data IS NULL AND lead_date >= '2025-04-25'
            ORDER BY lead_date, stock_name ;
        """)

        leads = cur.fetchall()
        
        print(f"Total leads fetched: {len(leads)}")



        # --- 3. Load Data from API ---
        def fetch_signal_data(stock_name, lead_date):
            url = "https://stapi02.azurewebsites.net/api/httpstsignals"

            # Convert lead_date to datetime and add one day to it
            lead_date = pd.to_datetime(lead_date)  # Ensure lead_date is in datetime format
            end_date = lead_date + timedelta(days=1)  # Add one day to the lead_date

            # Convert end_date back to string format for the API request
            end_date_str = end_date.strftime('%Y-%m-%d')

            params = {
                "code": "TryM8ecL_3NA8n8CtLwgowLvm08BAHpC3Xp4_QwxtqTKAzFugvz0LQ==",  # Replace with your code
                "name": stock_name,
                "start_date": "2019-01-01",  # Start date fixed
                "end_date": end_date_str  # Use the new end_date with +1 day
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()
                df = pd.DataFrame(data)

                # Ensure 'Date' column exists and convert it to datetime if necessary
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Convert to datetime, handle errors

                    # Filter the dataframe for the specified lead_date
                    lead_date = pd.to_datetime(lead_date)  # Ensure lead_date is also in datetime format
                    filtered_df = df[df['Date'] == lead_date]

                    columns_to_pick = ['Volume', 'H14', 'H9', 'H21', 'h_50_s',
                                        'coincident_points', 'Maverick', 'Maverick_Alpha',
                                        'Top_Gun_A', 'Top_Gun_B']

                    # Check if all required columns exist
                    if all(col in filtered_df.columns for col in columns_to_pick):
                        selected_data = filtered_df[columns_to_pick]
                        return selected_data.to_dict(orient='records')  # List of dicts
                    else:
                        print(f"❌ Some required columns missing for {stock_name} on {lead_date}")
                        return None
                else:
                    print(f"❌ 'Date' column not found in the response for {stock_name} on {lead_date}")
                    return None

            except requests.exceptions.RequestException as e:
                print(f"❌ API request error for {stock_name} on {lead_date}: {e}")
                return None

        # --- 4. Loop through Leads and Fetch + Update Data ---
        for i, (lead_id, stock_name, lead_date) in enumerate(leads, start=1):
            print(f"🚀 ({i}) Fetching signal data for {stock_name} on {lead_date}")

            signal_data = fetch_signal_data(stock_name, lead_date)

            if signal_data:
                try:
                    # Uncomment the following code to update the database with the fetched data
                    cur.execute("""
                        UPDATE stocktrader.leads_1
                        SET signal_data = %s
                        WHERE id = %s
                    """, [json.dumps(signal_data), lead_id])

                    conn.commit()
                    print(f"✅ Successfully updated signal_data for lead_id {lead_id} ({stock_name}, {lead_date}, {signal_data})")

                except Exception as db_error:
                    print(f"❌ Database update error for lead_id {lead_id}: {db_error}")
                    conn.rollback()

            else:
                print(f"⚠️ No valid signal data for {stock_name} on {lead_date}. Skipping...")

        # --- 5. Close Database Connection ---
        cur.close()
        conn.close()
        print("\n🎯 All done successfully!")

    signal_data_update = signal_data_update()


leads_phases_script_dag = signal_data_update_taskflow_api()
