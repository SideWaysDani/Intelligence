

import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['leads_downtrend_generate dag'])
def leads_generate_taskflow_api():

    @task()
    def lead_dag():


        import psycopg2
        import pandas as pd
        from sqlalchemy import create_engine, text
        import datetime
        # connect object returns the active connection. In future, we can select the connection from the pool.

        def connect():
            """ Connect to the PostgreSQL database server """
            conn = None
            try:
                print("In connect")
                # read connection parameters
                params = config()

                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                conn = psycopg2.connect(**params)

                return conn
            except(Exception, psycopg2.DatabaseError) as error:
                print(error)


        def closeconnection(conn):
            if conn is not None:
                conn.close()
                print('Database connection closed.')


        def connect1():
            """ Connect to the PostgreSQL database server """
            conn = None
            try:
                # read connection parameters
                params = config()

                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                conn = psycopg2.connect(**params)

                # create a cursor
                cur = conn.cursor()

                # execute a statement
                print('PostgreSQL database version:')
                cur.execute('SELECT version()')

                # display the PostgreSQL database server version
                db_version = cur.fetchone()
                print(db_version)

                # close the communication with the PostgreSQL
                cur.close()
            except(Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn is not None:
                    conn.close()
                    print('Database connection closed.')


        def config():
            """ Return the hardcoded database configuration parameters """
            return {
            'host': 'sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com',
                'database': 'postgres',
                'user': 'stpostgres',
                'password': 'stocktrader'
            }

        # and ticker in ('STZ','FAF')
        def fetch_all_companiesFortune1000():
            conn = None
            sql = """SELECT ticker FROM stocktrader.fortune_1000 where ticker not in ('NaN') and status_downtrend = 'Yes' order by ticker desc;"""
            try:
                # read database configuration
                params = config()
                # connect to the PostgreSQL database
                conn = psycopg2.connect(**params)
                # create a new cursor
                cur = conn.cursor()
                # execute the INSERT statement
                # print('inserted',date)
                cur.execute(sql)
                # get the generated id back
                # employee_id = cur.fetchone()[0]
                result = cur.fetchall()
                # commit the changes to the database
                conn.commit()
                # close communication with the database
                cur.close()
                print(result)
            except(Exception, psycopg2.DatabaseError) as error:
                print("Check sector Database error: ", error)
                return "Check sector Database error"
            finally:
                if conn is not None:
                    conn.close()
            print(result)
            return result

        from pandas.tseries.offsets import Day, BDay
        from datetime import datetime

        def isBusinessDay(date):
            bday = BDay()
            is_business_day = bday.is_on_offset(date)
            print(is_business_day)

            print(date)
            is_business_day = bday.is_on_offset(date)

            print(is_business_day)
            return is_business_day

        def nextBusinessDay(date):
            bday = BDay()
            return date + 1 * bday

        import datetime
        import sys

        from self import self

        # sys.path.insert(1, r'F:\TransBoxAus\harpoon\dags\src\utility')
        # from dags.src.utility.db_connect import *

        # from dags.src.utility.db_connect import *
        # from dags.src.data.utility.stockdata_helper_polygonio import setup, setup_houlry, get_closing_value_for_date
        from datetime import timedelta
        import pandas


        class LeadsMaverickHelper :


            def update_prices(self):
                try:
                    # Fetch rows of matured leads from the database
                    matured_leads = LeadsMaverickHelper.select_matured_leads(self)

                    # read database configuration
                    params = config()
                    # connect to the PostgreSQL database
                    conn = psycopg2.connect(**params)
                    # create a new cursor
                    cur = conn.cursor()

                    for lead in matured_leads:
                        lead_id, stock_name, lead_date, sealing_date = lead
                        # Fetch the closing price at lead_date and sealing_date using Polygon.io
                        lead_price = get_closing_value_for_date(stock_name, lead_date)
                        sealing_price = get_closing_value_for_date(stock_name, sealing_date)

                        if lead_price is not None and sealing_price is not None:
                            # Update the leads table with lead_price and sealing_price
                            cur.execute("UPDATE stocktrader.leads SET lead_price = %s, sealing_price = %s WHERE id = %s;",
                                            (lead_price, sealing_price, lead_id))
                            conn.commit()
                        else:
                            print(f"Error fetching prices for {stock_name} on {lead_date} or {sealing_date}")
                except Exception as e:
                    print(f"Error inserting lead: {str(e)}")
                finally:
                    if conn is not None:
                        conn.close()


            def select_matured_leads(self):
                try:
                    # read database configuration
                    params = config()
                    # connect to the PostgreSQL database
                    conn = psycopg2.connect(**params)
                    # create a new cursor
                    cur = conn.cursor()
                    # SQL query to select rows with a null sealing_flag
                    select_query = "SELECT id, stock_name, lead_date, sealing_date FROM stocktrader.leads WHERE sealing_flag = 'yes';"
                    cur.execute(select_query)

                    rows = cur.fetchall()

                    return rows
                except Exception as e:
                    print(f"Error inserting lead: {str(e)}")
                finally:
                    if conn is not None:
                        conn.close()


            from datetime import datetime, timedelta

            def insert_lead(self, stock_name, lead_date):
                try:
                    # Convert lead_date to datetime if it's in string format
                    if isinstance(lead_date, str):
                        lead_date = datetime.datetime.strptime(lead_date, "%Y-%m-%d")  # Adjust format if necessary

                    threshold_date = lead_date - timedelta(days = 7)

                    # Read database configuration
                    params = config()
                    # Connect to the PostgreSQL database
                    conn = psycopg2.connect(**params)
                    # Create a new cursor
                    cur = conn.cursor()
                    # Check if a record with the same stock_name and lead_date exists
                    cur.execute(
                        "SELECT id FROM stocktrader.leads_downtrend_1 WHERE stock_name = %s AND lead_date >= %s AND lead_date <= %s",
                        (stock_name, threshold_date, lead_date)
                    )
                    existing_record = cur.fetchone()

                    if existing_record:
                        print(f"Lead for {stock_name} on {lead_date} already exists. Skipping insertion.")
                    else:
                        # Insert the lead into the table
                        insert_sql = "INSERT INTO stocktrader.leads_downtrend_1 (stock_name, lead_date) VALUES (%s, %s)"
                        cur.execute(insert_sql, (stock_name, lead_date))
                        conn.commit()
                        print(f"Lead for {stock_name} on {lead_date} inserted successfully.")
                except Exception as e:
                    print(f"Error inserting lead: {str(e)}")
                finally:
                    if conn is not None:
                        conn.close()



        # import datetime
        # import math
        # from matplotlib import pyplot as plt
        # import numpy as np
        # import pandas as pd
        # from scipy import integrate
        # import matplotlib.dates
        # from scipy.signal import argrelextrema
        # from numpy.polynomial import polynomial as P
        # from self import self

        # # from dags.src.data.signals.TopGun_Maverick import TopGun_Maverick

        # def find_distance(x1, y1, x2, y2):
        #     distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
        #     return distance


        # def find_angle_of_reflection(input_column, index):
        #     angle_of_reflection = (math.degrees(
        #         math.atan(((input_column[index+1]) - (input_column[index]))/(3))))
        #     return angle_of_reflection


        # def find_angle_of_incidence(input_column, index):
        #     angle_of_incidence = (math.degrees(
        #         math.atan(((input_column[index]) - (input_column[index-1]))/(3))))
        #     return angle_of_incidence


        # def find_slope_and_intercept(x2, y2, x1, y1):
        #     slope = (y2-y1)/(x2-x1)
        #     intercept = y1 - (slope*x1)
        #     return slope, intercept


        # def find_area_between_two_lines(slope1, intercept1, slope2, intercept2, index):
        # # a = 0  # Lower limit
        # # b = 1  # Upper limit
        # # result, error = integrate.quad(my_function, a, b)
        #     area = integrate.quad(lambda x: slope1*x+intercept1 -
        #                           slope2*x-intercept2, index, index+1)
        #     return area[0]


        # def calculate_fuel(input_columnA, input_columnB, first_potential_fuel_index, last_potential_fuel_index, first_point_check):
        # # lets say input column1 is TopGunA (Blue) and input column2 is TopGunB (Orange)
        #     input_columnA = np.array(input_columnA)
        #     input_columnB = np.array(input_columnB)

        #     fuel = []
        #     length = len(input_columnA)
        #     for i, j in zip(range(first_potential_fuel_index, last_potential_fuel_index), range(len(input_columnB))):
        # # print("i: ", i)
        # # print("j: ", j)
        # # print("input_columnB[i]: ", input_columnB[j])
        # # print("input_columnB[i+1]: ", input_columnB[j+1])
        #         slope_of_A, intercept_of_A = find_slope_and_intercept(
        #             i+1, input_columnA[i+1], i, input_columnA[i])
        #         slope_of_B, intercept_of_B = find_slope_and_intercept(
        #             j+1, input_columnB[j+1], j, input_columnB[j])
        #         area = find_area_between_two_lines(
        #             slope_of_A, intercept_of_A, slope_of_B, intercept_of_B, i)

        #         print("area", area)
        #         if i == 0 or first_point_check == True:
        #             fuel.append(area)

        #         else:
        #             fuel.append((-1*area)+fuel[i-1])

        # # print("area: ", area)
        # # print("fuel: ", fuel)

        #     fuel.append(fuel[len(fuel)-1])
        #     return fuel


        # def area_between_fuel_potential_signals(fuel, orange_potential_fuel_signals):

        #     total_area_bw_2fuel_sig = []
        #     Straight_line_points = []
        #     first_point_check = True

        #     count = 0
        #     for i in range(len(orange_potential_fuel_signals)-1):
        #         slope_of_orange_points, intercept_of_orange_points = find_slope_and_intercept(
        #             orange_potential_fuel_signals[i+1], fuel[orange_potential_fuel_signals[i+1]], orange_potential_fuel_signals[i], fuel[orange_potential_fuel_signals[i]])
        #         if count == 0:
        #             for j in range(orange_potential_fuel_signals[i], orange_potential_fuel_signals[i+1]+1):
        #                 Straight_line_points.append(
        #                     slope_of_orange_points*j+intercept_of_orange_points)
        # # print('i',i)
        # # print('J',j)
        # # print('count',count+15)
        # # print(fuel[j])
        # # print(Straight_line_points[count])
        #                 print('check if value of straight line pont is same as value',
        #                       Straight_line_points[count] == fuel[j])
        #                 print('straight_line_points', Straight_line_points)
        #                 count += 1
        #         else:
        #             for j in range(orange_potential_fuel_signals[i]+1, orange_potential_fuel_signals[i+1]+1):
        #                 Straight_line_points.append(
        #                     slope_of_orange_points*j+intercept_of_orange_points)

        # # print('i',i)
        # # print('J',j)
        # # print('count',count+15)
        #                 print(fuel[j])
        #                 print(Straight_line_points[count])
        #                 print('check if value of straight line pont is same as value',
        #                       Straight_line_points[count] == fuel[j])
        #                 print('straight_line_points', Straight_line_points)
        #                 count += 1

        #     fuel_bw_2points = calculate_fuel(
        #         fuel, Straight_line_points, orange_potential_fuel_signals[0], orange_potential_fuel_signals[-1], first_point_check)

        #     fuel_bw_2points = [-x for x in fuel_bw_2points]

        #     total_area_bw_2fuel_sig += fuel_bw_2points

        #     print("total_area_bw_2fuel_sig: ", total_area_bw_2fuel_sig)
        #     print(np.count_nonzero(np.array(total_area_bw_2fuel_sig == 0)))
        #     print("len(total_area_bw_2fuel_sig): ", len(total_area_bw_2fuel_sig))
        #     print("\n")
        #     print("\n")
        #     return total_area_bw_2fuel_sig, Straight_line_points


        # def find_fuel_signal_consistency_frequency(potential_fuel_signal):
        #     to_check_days_threshold = 14
        #     fuel_signal_consistency_frequency = [0]*len(potential_fuel_signal)

        #     for i in range(len(potential_fuel_signal)):
        #         if potential_fuel_signal[i] == 1:
        #             print("i: this i is in outer loop", i)

        #             for j in range(1,to_check_days_threshold):
        #                 if potential_fuel_signal[i-j] == 1:
        #                     print("i: ", i)
        #                     print("j: ", j)
        #                     print("i-j: ", i-j)
        #                     print("fuel_signal_consistency_frequency[i-j]: ", fuel_signal_consistency_frequency[i-j])
        #                     fuel_signal_consistency_frequency[i] = fuel_signal_consistency_frequency[i-j]+1
        #                     print("New fuel_signal_consistency_frequency[i-j]: ", fuel_signal_consistency_frequency[i-j])
        #                     break

        #                 else:
        #                     fuel_signal_consistency_frequency[i] = 1

        #         else:
        #             fuel_signal_consistency_frequency[i] = 0

        #     return fuel_signal_consistency_frequency



        # def find_maximas(input_column):
        #     maximas_indices = argrelextrema(np.array(input_column), np.greater)[0]
        #     mean_of_maximas = np.mean(np.array(input_column)[maximas_indices])
        #     threshold = mean_of_maximas * 1.1
        #     for i in maximas_indices:
        #         if input_column[i] < threshold:
        #             maximas_indices = np.delete(
        #                 maximas_indices, np.where(maximas_indices == i))

        #     potential_fuel_signal = [
        #         1 if i in maximas_indices else 0 for i in range(len(input_column))]

        #     return maximas_indices, potential_fuel_signal


        # def find_overlapping_points(input_column1, input_column2):

        # # first_non_nan_index = input_column1.first_valid_index()
        #     first_non_nan_index = 0
        #     for index, value in enumerate(input_column1):
        #         if not np.isnan(value):  # Replace 'None' with your specific non-value indicator
        #             first_non_nan_index = index
        #             break
        # # print("first_non_nan_index: ", first_non_nan_index)
        #     overlapping_points = []
        #     input_column1_high = False

        #     if input_column1[0] > input_column2[0]:
        #         input_column1_high = True

        #     for i in range(first_non_nan_index, len(input_column1)):
        #         if input_column1_high == False:
        #             if input_column1[i] >= input_column2[i]:
        #                 overlapping_points.append(i)
        #                 input_column1_high = True
        #         else:
        #             if input_column1[i] <= input_column2[i]:
        #                 overlapping_points.append(i)
        #                 input_column1_high = False
        # # print("overlapping_points: ", overlapping_points)
        #     return overlapping_points



        import datetime
        import numpy as np
        import pandas as pd
        import matplotlib.pyplot as plt
        from scipy.signal import argrelextrema
        from scipy import integrate
        import requests
        from datetime import datetime
        import pandas as pd
        import requests

        import pandas as pd
        import datetime
        import requests

        import pandas as pd

        import requests
        import datetime
        import pandas as pd
        import requests




        def calculate_fuel(input_columnA, input_columnB):
            """Calculate fuel status as the area between two lines, using ROC for responsiveness"""
            fuel = [0]  # Initialize fuel with a starting value (0 or any suitable initial value)
            rate_of_change = np.diff(input_columnA - input_columnB)  # Difference between successive points

            # Apply EMA to rate_of_change for smoother, more responsive adjustment
            smoothed_roc = ema_smoothing(rate_of_change, span=30)  # 15-day smoothing span for better responsiveness

            # Accumulate the smoothed rate of change into the fuel list
            for i in range(1, len(smoothed_roc)):  # Starting from index 1 since 0 is already initialized
                fuel.append(fuel[-1] + smoothed_roc[i - 1])  # Append new value based on the last item in fuel

            return np.array(fuel) * -1  # Ensure the signal is properly reversed if needed

        # Helper functions
        def find_slope_and_intercept(x1, y1, x2, y2):
            """Calculate slope and intercept between two points"""
            slope = (y2 - y1) / (x2 - x1)
            intercept = y1 - slope * x1
            return slope, intercept

        def detect_downtrend_regions(fuel_status, threshold_slope=-0.005, min_duration=2):
            """Detect continuous regions of decline based on slope, adjusting to capture more downtrends."""
            downtrend_regions = []
            start = None

            for i in range(1, len(fuel_status)):
                slope = fuel_status[i] - fuel_status[i - 1]

                if slope < threshold_slope:  # Check for decline
                    if start is None:
                        start = i - 1  # Mark the start of the downtrend
                else:
                    if start is not None:  # End of downtrend
                        # Only add the downtrend region if it lasts at least `min_duration` days
                        if (i - start) >= min_duration:
                            downtrend_regions.append((start, i - 1))
                        start = None

            # **Key Fix: Check for downtrend that continues till the very last point**
            if start is not None:
                # If the trend is still ongoing at the end of the data, consider the last day as part of the trend
                downtrend_regions.append((start, len(fuel_status) - 1))

            return downtrend_regions

        # EMA Smoothing function for more responsiveness
        def ema_smoothing(signal, span=15):
            """Apply Exponential Moving Average (EMA) smoothing to the signal."""
            return pd.Series(signal).ewm(span=span, adjust=False).mean()

        def mark_downtrend_in_dataframe(df, downtrend_regions):
            """Mark downtrend and non-downtrend regions in the DataFrame."""
            df['output_signal'] = 0  # Initialize the column with 0s

            # Mark downtrend regions as 1 (pink signal)
            for start_idx, end_idx in downtrend_regions:
                df.loc[start_idx:end_idx, 'output_signal'] = 1  # Mark the downtrend regions as 1

            # Create additional columns for start_date, end_date, and signal
            signals = []
            current_position = 0
            for start_idx, end_idx in downtrend_regions:
                # Mark non-downtrend regions between downtrends as 0
                if current_position < start_idx:
                    signals.append({
                        'start_date': df.iloc[current_position]['Date'],
                        'end_date': df.iloc[start_idx - 1]['Date'],
                        'signal': 0  # Non-downtrend region
                    })
                # Mark downtrend region as 1
                signals.append({
                    'start_date': df.iloc[start_idx]['Date'],
                    'end_date': df.iloc[end_idx]['Date'],
                    'signal': 1  # Downtrend region
                })
                current_position = end_idx + 1

            # Add the final non-downtrend region after the last downtrend region (if any)
            if current_position < len(df):
                signals.append({
                    'start_date': df.iloc[current_position]['Date'],
                    'end_date': df.iloc[len(df) - 1]['Date'],
                    'signal': 0  # Non-downtrend region
                })

            # Convert signals to a DataFrame for easy display or export
            signal_df = pd.DataFrame(signals)

            return df, signal_df

        def plot_combined_figures(dates, maverick, maverick_alpha, fuel_status, smoothed_fuel_status, downtrend_regions, max_fuel_points, min_fuel_points, lead_dates, signal_df):
            """Plot Maverick vs Maverick Alpha and Fuel Signal with Downtrend Regions on the same figure."""

            # Create a figure with two subplots
            fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

            # 1st Plot: Maverick vs Maverick Alpha
            axes[0].plot(dates, maverick, label='Maverick', color='blue', alpha=0.7)
            axes[0].plot(dates, maverick_alpha, label='Maverick Alpha', color='green', alpha=0.7)
            axes[0].set_title('Maverick vs Maverick Alpha')
            axes[0].set_ylabel('Values')
            axes[0].legend()
            axes[0].grid(True)

            # 2nd Plot: Fuel Signal with Downtrend Regions
            dates_list = dates.tolist()  # Convert dates to list for indexing
            axes[1].plot(dates_list, fuel_status, label='Original Fuel Status', color='blue', alpha=0.7)
            axes[1].plot(dates_list, smoothed_fuel_status, label='Smoothed Fuel Status (EMA)', color='green')

            # Highlight downtrend regions
            for region in downtrend_regions:
                start_idx, end_idx = region
                if 0 <= start_idx < len(dates_list) and 0 <= end_idx < len(dates_list) and start_idx != end_idx:
                    axes[1].axvspan(dates_list[start_idx], dates_list[end_idx], color='pink', alpha=0.5, label='Downtrend Region' if 'Downtrend Region' not in axes[1].get_legend_handles_labels()[1] else "")

            # Mark maxima and minima
            axes[1].scatter([dates_list[i] for i in max_fuel_points], [fuel_status[i] for i in max_fuel_points], c='orange', label='Maxima')
            axes[1].scatter([dates_list[i] for i in min_fuel_points], [fuel_status[i] for i in min_fuel_points], c='red', label='Minima')

            # Plot lead dates with specific colors based on their signal (1 = black, 0 = blue)
            for lead_date in lead_dates:
                lead_date_index = dates_list.index(lead_date) if lead_date in dates_list else None
                if lead_date_index is not None:
                    signal_row = signal_df[(signal_df['start_date'] <= lead_date) & (signal_df['end_date'] >= lead_date)]
                    if not signal_row.empty and signal_row['signal'].iloc[0] == 1:
                        axes[1].scatter(dates_list[lead_date_index], smoothed_fuel_status[lead_date_index], color='black', label='Lead Date (Signal 1)', zorder=5)
                    else:
                        axes[1].scatter(dates_list[lead_date_index], smoothed_fuel_status[lead_date_index], color='blue', label='Lead Date (Signal 0)', zorder=5)

            # Remove duplicate legends for lead dates
            handles, labels = axes[1].get_legend_handles_labels()
            unique_labels = dict(zip(labels, handles))
            axes[1].legend(unique_labels.values(), unique_labels.keys())

            axes[1].set_title('Fuel Signal with Downtrend Regions')
            axes[1].set_xlabel('Date')
            axes[1].set_ylabel('Fuel Status')
            axes[1].grid(True)

            # Adjust layout
            plt.tight_layout()
            plt.show()


        def check_lead_date_against_downtrend(signal_df, lead_dates):
            """
            Check if lead dates fall within any downtrend region.
            Print 1 if the date is in a downtrend region, else 0.
            """
            for lead_date in lead_dates:
                # Use `format='mixed'` to handle inconsistent formats
                try:
                    lead_date = pd.to_datetime(lead_date, format='mixed', dayfirst=True)
                except ValueError:
                    print(f"Error parsing date: {lead_date}. Please check the input format.")
                    continue  # Skip the problematic date

                found_in_downtrend = 0  # Default to not found
                for _, row in signal_df.iterrows():
                    if row['signal'] == 1 and row['start_date'] <= lead_date <= row['end_date']:
                        found_in_downtrend = 1
                        break
                print(f"{lead_date}: {found_in_downtrend}")








        def mainn(df):

            # end_date = "2024-12-31"  # Keep as string to match the existing function
            # stock_name = "ZTS"

            # # Load data using the existing function
            # df = load_data_from_api(stock_name, end_date)
            df=df
            # Check if data was successfully loaded
            if df is None:
                print("Failed to load data from the API. Exiting...")
                return None, None, None

            # Proceed if data is loaded successfully
            df = df.dropna(subset=['Maverick', 'Maverick_Alpha'])

            # Calculate fuel status
            Maverick = np.array(df['Maverick'])
            MaverickAlpha = np.array(df['Maverick_Alpha'])
            fuel_status = calculate_fuel(Maverick, MaverickAlpha)

            # Adjust fuel_status length to match df['Date']
            if len(fuel_status) < len(df):
                fuel_status = np.pad(fuel_status, (1, 0), 'constant', constant_values=(0, 0))
            elif len(fuel_status) > len(df):
                fuel_status = fuel_status[:len(df)]

            # Apply EMA smoothing instead of moving average
            smoothed_fuel_status = ema_smoothing(fuel_status, span=1)

            # Detect downtrend regions with lower threshold and longer duration
            downtrend_regions = detect_downtrend_regions(smoothed_fuel_status, threshold_slope=-0.005, min_duration=2)

            # Mark downtrend regions in the DataFrame
            df, signal_df = mark_downtrend_in_dataframe(df, downtrend_regions)

            # Find maxima and minima
            max_fuel_points = argrelextrema(smoothed_fuel_status.values, np.greater)[0]
            min_fuel_points = argrelextrema(smoothed_fuel_status.values, np.less)[0]

            # Load lead dates from CSV
            # leads_df = pd.read_csv('leads.csv')
            # lead_dates = pd.to_datetime(leads_df['lead_date'], dayfirst=True).tolist()

            # Plot both figures
            # plot_combined_figures(
            #     df['Date'], Maverick, MaverickAlpha, fuel_status, smoothed_fuel_status,
            #     downtrend_regions, max_fuel_points, min_fuel_points, lead_dates, signal_df
            # )

            # Display the output signal DataFrame
            # print(signal_df)

            # Extract the last 10 rows of data
            df.sort_values(by='Date', ascending=True, inplace=True)
            dff = df[-1:]
            received = False
            det_date = None
            # Check if any 'output_signal' value is 1 in the last 10 rows
            if (dff['output_signal'] == 1).any():
                det_date_df = df[dff['output_signal'].reindex(df.index, fill_value=False) == 1]
                row_det_date_df = det_date_df.iloc[-1]
                det_date = row_det_date_df['Date']
                det_date = pd.Timestamp(det_date)
                det_date = det_date.strftime('%Y-%m-%d')
                received = True
            print(received)
            return received,det_date










        def main(df):

            df = df.dropna(subset=['Maverick', 'Maverick_Alpha'])

        # Plot the original series
            Maverick = np.array(df['Maverick'])
            MaverickAlpha = np.array(df['Maverick_Alpha'])
            columns_to_be_filtered = [Maverick, MaverickAlpha]
            fuel_status = calculate_fuel(Maverick, MaverickAlpha, 0, len(Maverick) - 1, False)
            df['fuel_status'] = fuel_status

            max_fuel_points, potential_fuel_signal = find_maximas(fuel_status)
            df['potential_fuel_signal'] = potential_fuel_signal

        # print("max_fuel_points: ", max_fuel_points)
            max_fuel_values = np.take(fuel_status, max_fuel_points)

            total_area_bw_2fuel_sig, straight_line_points = area_between_fuel_potential_signals(fuel_status, max_fuel_points)
            fuel_signal_consistency_frequency = find_fuel_signal_consistency_frequency(potential_fuel_signal)
        # df['fuel_signal_consistency_frequency'] = fuel_signal_consistency_frequency

        # saving the dataframe to csv file
            # df.to_csv('output file with fuel elements.csv')

        # Check if the last seven entries of the output_signal_max_fuel contain 1
            recieved = False
            if any(fuel_signal_consistency_frequency[-3:]):
                recieved = True

        # fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, sharex=True)
        # ax1.plot(df['Date'], Maverick, label='A')
        # ax1.plot(df['Date'], MaverickAlpha, label='B')

        # filtered_points_wrt_angles,filtered_indices = finding_and_filtering_angles(df,columns_to_be_filtered)

        # ax1.scatter(maximas_indices, df.loc[maximas_indices, 'TopGunA'], c='green', label='local maximas')
        # ax1.scatter(minimas_indices, df.loc[minimas_indices, 'TopGunA'], c='red', label='local minimas')
        # ax1.set_title('Msverick and MaverickAlpha')

        # ax1.scatter(sweet_spots_indices, df.loc[points_in_zone_indices, 'Sonar'], c='red', label='Sweet spots')
        # ax1.legend()

        # ax3.plot(final_potential_points, label='potential_points also catering angle difference')


        # print("fuel_status: ", fuel_status)

        # ax2.plot(fuel_status, label='fuel_status')


        # ax2.plot(range(max_fuel_points[0], max_fuel_points[-1] + 1), straight_line_points, label='straight_line_points')
        # ax2.scatter(max_fuel_points, max_fuel_values, c='orange', label='fuel_signal')
        # ax2.legend()
        #
        # ax3.plot(potential_fuel_signal, label="potential_fuel_signal")
        # ax3.legend()



                print("Recieved:", recieved)

        # ax4.plot(range(max_fuel_points[0],(max_fuel_points[-1])),total_area_bw_2fuel_sig, label='total_area_between_orange_points')
        # ax4.plot(df['Date'], fuel_signal_consistency_frequency, label='fuel_signal_consistency_frequency')
        # ax4.legend()
        # print("max_fuel_points: ", max_fuel_points)
        # print("fuel_signal_consistency_frequency: ", fuel_signal_consistency_frequency)
        # plt.show()

            return recieved




        def load_data_from_api(stock_name, enday):
            """
            Fetch stock data from the API and ensure full date alignment.
            Handles both string and datetime formats for end_date, and hardcodes start_date within the function.

            Parameters:
                stock_name (str): The name of the stock.
                enday (str or datetime): The end date for data retrieval (supports both formats).

            Returns:
                pd.DataFrame: A DataFrame containing the stock data with full date alignment.
            """
            try:
                # Check if enday is a string in a date-like format
                if isinstance(enday, str):
                    try:
                        # Attempt to parse the string into a date
                        end_date_parsed = pd.to_datetime(enday)
                        formatted_end_date = end_date_parsed.strftime('%Y-%m-%d')
                    except Exception as e:
                        print(f"Failed to parse end_date string: {e}")
                        return None
                elif isinstance(enday, (pd.Timestamp, datetime.datetime, datetime.date)):
                    # If enday is already a datetime or date object
                    formatted_end_date = enday.strftime('%Y-%m-%d')
                else:
                    print("Unsupported format for enday. Please provide a valid date string or date object.")
                    return None

                # Define start_date directly inside the function
                start_date = "2020-01-01"

                # Construct the API URL and parameters
                url = "https://stapi02.azurewebsites.net/api/httpstsignals"
                params = {
                    "code": "TryM8ecL_3NA8n8CtLwgowLvm08BAHpC3Xp4_QwxtqTKAzFugvz0LQ==",
                    "name": stock_name,
                    "start_date": start_date,
                    "end_date": formatted_end_date,
                }
                headers = {"Content-Type": "application/json"}

                # Make the API call
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)

                # Convert response JSON to a pandas DataFrame
                data = pd.DataFrame(response.json())
                data['Date'] = pd.to_datetime(data['Date'])

                # Ensure the data covers the full date range
                full_date_range = pd.date_range(start=start_date, end=formatted_end_date, freq='D')  # Daily frequency
                data = data.set_index('Date').reindex(full_date_range).reset_index()
                data.rename(columns={'index': 'Date'}, inplace=True)

                # Handle missing values (optional: interpolate or fill with zero)
                data = data.ffill().bfill()  # Forward fill followed by backward fill

                return data

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                print(f"Request error occurred: {req_err}")
            except Exception as e:
                print(f"An error occurred: {e}")

            return None


        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError

        def insert_detect_date(date, end_date, stock_name):
            # Database connection URL
            db_url = 'postgresql://stpostgres:stocktrader@sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com:5432/postgres'

            # Create the SQLAlchemy engine
            engine = create_engine(db_url)

            # SQL query to update the detected_date
            update_query = text("""
                UPDATE stocktrader.leads_downtrend_1
                SET detected_date = :date
                WHERE lead_date = :end_date AND stock_name = :stock_name;
            """)

            # SQL query to check matching records
            check_query = text("""
                SELECT * FROM stocktrader.leads_downtrend_1
                WHERE lead_date = :end_date AND stock_name = :stock_name;
            """)

            try:
                with engine.begin() as connection:
                    # Check matching records
                    result = connection.execute(check_query, {"end_date": end_date, "stock_name": stock_name}).fetchall()
                    print("Matching records before update:", result)

                    # Execute the update query
                    update_result = connection.execute(update_query, {"date": date, "end_date": end_date, "stock_name": stock_name})
                    print(f"Rows affected: {update_result.rowcount}")
                    print(f"Update detected date {date} successful.")
            except SQLAlchemyError as e:
                print(f"An error occurred: {e}")



        def generate_leadsFortune1000(end_date):
        # Fetch all companies
            companies = fetch_all_companiesFortune1000()
        # print("hello")
            for company in companies:

        # print("Stock name = ", company[0], )
                try:
                    stock_name = company[0]
                    start_date = datetime.datetime(2020, 1, 1)

                    threshold = 0.02
                    sharp_rise_threshold = 0.01  # Define the threshold for a sharp rise

        # df = TopGun_Maverick.setup_specific(self, stock_name, start_date, end_date)
                    df=load_data_from_api(stock_name,end_date)

        # Check if the last seven entries of the output_signal_max_fuel contain 1
                    recieved,det_date = mainn(df)


                    print("Recieved:", det_date)


        # plt.show()D
                    if recieved:
                        # print(recieved)
        # print(stock_name)
                        helper = LeadsMaverickHelper()
                        helper.insert_lead(stock_name, end_date)
                        insert_detect_date(det_date, end_date, stock_name)
                    # else:
                        # print(recieved)

                except Exception as e:
        # print("Exception: " + str(e))
                    pass

        def generate_leads(start_date, end_date):
            day_count = (end_date - start_date).days + 1
            for single_date in [d for d in (start_date + datetime.timedelta(n) for n in range(day_count)) if d <= end_date]:
                if isBusinessDay(single_date):

                    formatted_date = single_date.strftime("%Y-%m-%d")
                    generate_leadsFortune1000(formatted_date)
                else:
                    formatted_date = single_date.strftime("%Y-%m-%d")
                    print("Not a business day :", formatted_date)

        def determine_leads_end(start_date, end_date):
            day_count = (end_date - start_date).days + 1
            for single_date in [d for d in (start_date + datetime.timedelta(n) for n in range(day_count)) if d <= end_date]:
                if isBusinessDay(single_date):
        # print("++ Business day :", single_date)
                    update_leads_table(single_date)
                else:
                    print("Not a business day :", single_date)




        # Function to calculate and add prices to leads table
        def calculate_prices():

            LeadsMaverickHelper.update_prices(self)

        def orchestrate_leads(start_date,end_date):



            generate_leads(start_date, end_date)


        # start_date = datetime.datetime(2023, 10, 13)
        # end_date = datetime.datetime(2023, 11, 30)

        # etermine_leads_end(start_date, end_date)

            calculate_prices()


        
        st = datetime.datetime(2023, 1, 1)
        en = datetime.datetime(2023, 12, 1)
        orchestrate_leads(st,en)
        # insert_leads(st,en)
        connect()
    lead_dag = lead_dag()


leads_phases_script_dag = leads_generate_taskflow_api()
