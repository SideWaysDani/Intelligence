

import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'retry': 1,
    'retry_delay': timedelta(minutes=2)
}


# default_args = {
#     'owner': 'airflow',
# }

@dag(default_args=default_args, schedule_interval="@daily", start_date=days_ago(2), tags=['all_tasks_leads_generate dag'])
# @dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['leads_generate dag'])
def all_tasks_generate_taskflow_api():

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


        def fetch_all_companies():
            conn = None
            sql = """SELECT stock_symbol FROM stocktrader.stocks;"""
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
            return result

        def fetch_all_companiesFortune1000India():
            conn = None
            sql = """SELECT ticker FROM stocktrader.fortune_1000_India where ticker not in ('NaN') and status = 'Yes' order by ticker desc;"""
            try:
                # read database configuration
                params = config()
                # connect to the PostgreSQL database
                conn = psycopg2.connect(**params)
                # create a new cursor
                cur = conn.cursor()
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


        # and ticker in ('STZ','FAF')
        def fetch_all_companiesFortune1000():
            conn = None
            sql = """SELECT ticker FROM stocktrader.fortune_1000 where ticker not in ('NaN') order by ticker desc;"""
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
        def fetch_all_companiesFortune_crypto():
            conn = None
            sql = """SELECT ticker FROM stocktrader.fortune_crypto where ticker not in ('NaN') and status = 'Yes' order by ticker desc;"""
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

            def select_leads_table(self):
                try:
                    # read database configuration
                    params = config()
                    # connect to the PostgreSQL database
                    conn = psycopg2.connect(**params)
                    # create a new cursor
                    cur = conn.cursor()
                    # SQL query to select rows with a null sealing_flag
                    select_query = "SELECT * FROM stocktrader.leads WHERE sealing_flag IS NULL;"
                    cur.execute(select_query)

                    rows = cur.fetchall()

                    return rows
                except Exception as e:
                    print(f"Error inserting lead: {str(e)}")
                finally:
                    if conn is not None:
                        conn.close()

            def update_leads_table(lead_id, sealing_date):
                try:
                    # read database configuration
                    params = config()
                    # connect to the PostgreSQL database
                    conn = psycopg2.connect(**params)
                    # create a new cursor
                    cur = conn.cursor()

                    # Update the sealing_flag and sealing_date columns
                    update_query = "UPDATE stocktrader.leads SET sealing_flag = 'yes', sealing_date = %s WHERE id = %s;"
                    cur.execute(update_query, (sealing_date, lead_id))

                    conn.commit()
                    cur.close()

                    print("Table 'leads' updated successfully.")


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
                        "SELECT id FROM stocktrader.leads_1 WHERE stock_name = %s AND lead_date >= %s AND lead_date <= %s",
                        (stock_name, threshold_date, lead_date)
                    )
                    existing_record = cur.fetchone()

                    if existing_record:
                        print(f"Lead for {stock_name} on {lead_date} already exists. Skipping insertion.")
                    else:
                        # Insert the lead into the table
                        insert_sql = "INSERT INTO stocktrader.leads_1 (stock_name, lead_date) VALUES (%s, %s)"
                        cur.execute(insert_sql, (stock_name, lead_date))
                        conn.commit()
                        print(f"Lead for {stock_name} on {lead_date} inserted successfully.")
                except Exception as e:
                    print(f"Error inserting lead: {str(e)}")
                finally:
                    if conn is not None:
                        conn.close()

            # def insert_lead(self, stock_name, lead_date):

        # try:
        # threshold_date = lead_date - timedelta(days=7)
        #         # read database configuration
        # params = config() 
        #         # connect to the PostgreSQL database
        # conn = psycopg2.connect(**params)
        #         # create a new cursor
        # cur = conn.cursor()
        #         # Check if a record with the same stock_name and lead_date exists
        # cur.execute(
        #             "SELECT id FROM stocktrader.leads_2 WHERE stock_name = %s AND lead_date >= %s AND lead_date <= %s",
        #             (stock_name, threshold_date, lead_date))
        # existing_record = cur.fetchone()

        #if existing_record:
        # print(f"Lead for {stock_name} on {lead_date} already exists. Skipping insertion.")
        #else:
        #             # Insert the lead into the table
        # insert_sql = "INSERT INTO stocktrader.leads_2 (stock_name, lead_date) VALUES (%s, %s)"
        # cur.execute(insert_sql, (stock_name, lead_date))
        # conn.commit()
        # print(f"Lead for {stock_name} on {lead_date} inserted successfully.")
        # except Exception as e:
        # print(f"Error inserting lead: {str(e)}")
        # finally:
        #if conn is not None:
        # conn.close()

            def insert_lead_india(self, stock_name, lead_date):

                try:
                    threshold_date = lead_date - timedelta(days=7)
        # read database configuration
                    params = config()
        # connect to the PostgreSQL database
                    conn = psycopg2.connect(**params)
        # create a new cursor
                    cur = conn.cursor()
        # Check if a record with the same stock_name and lead_date exists
                    cur.execute(
                        "SELECT id FROM stocktrader.leads_India WHERE stock_name = %s AND lead_date >= %s AND lead_date <= %s",
                        (stock_name, threshold_date, lead_date))
                    existing_record = cur.fetchone()

                    if existing_record:
                        print(f"Lead for {stock_name} on {lead_date} already exists. Skipping insertion.")
                    else:
        # Insert the lead into the table
                        insert_sql = "INSERT INTO stocktrader.leads_India (stock_name, lead_date) VALUES (%s, %s)"
                        cur.execute(insert_sql, (stock_name, lead_date))
                        conn.commit()
                        print(f"Lead for {stock_name} on {lead_date} inserted successfully.")
                except Exception as e:
                    print(f"Error inserting lead: {str(e)}")
                finally:
                    if conn is not None:
                        conn.close()


            def insert_lead_hourly(self, stock_name, lead_date):

                try:
                    threshold_date = lead_date - timedelta(days=7)
        # read database configuration
                    params = config()
        # connect to the PostgreSQL database
                    conn = psycopg2.connect(**params)
        # create a new cursor
                    cur = conn.cursor()
        # Check if a record with the same stock_name and lead_date exists
                    cur.execute(
                        "SELECT id FROM stocktrader.leads_hour WHERE stock_name = %s AND lead_date >= %s AND lead_date <= %s",
                        (stock_name, threshold_date, lead_date))
                    existing_record = cur.fetchone()

                    if existing_record:
                        print(f"Lead for {stock_name} on {lead_date} already exists. Skipping insertion.")
                    else:
        # Insert the lead into the table
                        insert_sql = "INSERT INTO stocktrader.leads_hour (stock_name, lead_date) VALUES (%s, %s)"
                        cur.execute(insert_sql, (stock_name, lead_date))
                        conn.commit()
                        print(f"Lead for {stock_name} on {lead_date} inserted successfully.")
                except Exception as e:
                    print(f"Error inserting lead: {str(e)}")
                finally:
                    if conn is not None:
                        conn.close()


            def close_connection(self):
                self.conn.close()
        import datetime
        import math
        from matplotlib import pyplot as plt
        import numpy as np
        import pandas as pd
        from scipy import integrate
        import matplotlib.dates
        from scipy.signal import argrelextrema
        from numpy.polynomial import polynomial as P
        from self import self

        # from dags.src.data.signals.TopGun_Maverick import TopGun_Maverick

        def find_distance(x1, y1, x2, y2):
            distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
            return distance


        def find_angle_of_reflection(input_column, index):
            angle_of_reflection = (math.degrees(
                math.atan(((input_column[index+1]) - (input_column[index]))/(3))))
            return angle_of_reflection


        def find_angle_of_incidence(input_column, index):
            angle_of_incidence = (math.degrees(
                math.atan(((input_column[index]) - (input_column[index-1]))/(3))))
            return angle_of_incidence


        def find_slope_and_intercept(x2, y2, x1, y1):
            slope = (y2-y1)/(x2-x1)
            intercept = y1 - (slope*x1)
            return slope, intercept


        def find_area_between_two_lines(slope1, intercept1, slope2, intercept2, index):
        # a = 0  # Lower limit
        # b = 1  # Upper limit
        # result, error = integrate.quad(my_function, a, b)
            area = integrate.quad(lambda x: slope1*x+intercept1 -
                                    slope2*x-intercept2, index, index+1)
            return area[0]


        def calculate_fuel(input_columnA, input_columnB, first_potential_fuel_index, last_potential_fuel_index, first_point_check):
        # lets say input column1 is TopGunA (Blue) and input column2 is TopGunB (Orange)
            input_columnA = np.array(input_columnA)
            input_columnB = np.array(input_columnB)

            fuel = []
            length = len(input_columnA)
            for i, j in zip(range(first_potential_fuel_index, last_potential_fuel_index), range(len(input_columnB))):
        # print("i: ", i)
        # print("j: ", j)
        # print("input_columnB[i]: ", input_columnB[j])
        # print("input_columnB[i+1]: ", input_columnB[j+1])
                slope_of_A, intercept_of_A = find_slope_and_intercept(
                    i+1, input_columnA[i+1], i, input_columnA[i])
                slope_of_B, intercept_of_B = find_slope_and_intercept(
                    j+1, input_columnB[j+1], j, input_columnB[j])
                area = find_area_between_two_lines(
                    slope_of_A, intercept_of_A, slope_of_B, intercept_of_B, i)

                print("area", area)
                if i == 0 or first_point_check == True:
                    fuel.append(area)

                else:
                    fuel.append((-1*area)+fuel[i-1])

        # print("area: ", area)
        # print("fuel: ", fuel)

            fuel.append(fuel[len(fuel)-1])
            return fuel


        def area_between_fuel_potential_signals(fuel, orange_potential_fuel_signals):

            total_area_bw_2fuel_sig = []
            Straight_line_points = []
            first_point_check = True

            count = 0
            for i in range(len(orange_potential_fuel_signals)-1):
                slope_of_orange_points, intercept_of_orange_points = find_slope_and_intercept(
                    orange_potential_fuel_signals[i+1], fuel[orange_potential_fuel_signals[i+1]], orange_potential_fuel_signals[i], fuel[orange_potential_fuel_signals[i]])
                if count == 0:
                    for j in range(orange_potential_fuel_signals[i], orange_potential_fuel_signals[i+1]+1):
                        Straight_line_points.append(
                            slope_of_orange_points*j+intercept_of_orange_points)
        # print('i',i)
        # print('J',j)
        # print('count',count+15)
        # print(fuel[j])
        # print(Straight_line_points[count])
                        print('check if value of straight line pont is same as value',
                                Straight_line_points[count] == fuel[j])
                        print('straight_line_points', Straight_line_points)
                        count += 1
                else:
                    for j in range(orange_potential_fuel_signals[i]+1, orange_potential_fuel_signals[i+1]+1):
                        Straight_line_points.append(
                            slope_of_orange_points*j+intercept_of_orange_points)

        # print('i',i)
        # print('J',j)
        # print('count',count+15)
                        print(fuel[j])
                        print(Straight_line_points[count])
                        print('check if value of straight line pont is same as value',
                                Straight_line_points[count] == fuel[j])
                        print('straight_line_points', Straight_line_points)
                        count += 1

            fuel_bw_2points = calculate_fuel(
                fuel, Straight_line_points, orange_potential_fuel_signals[0], orange_potential_fuel_signals[-1], first_point_check)

            fuel_bw_2points = [-x for x in fuel_bw_2points]

            total_area_bw_2fuel_sig += fuel_bw_2points

            print("total_area_bw_2fuel_sig: ", total_area_bw_2fuel_sig)
            print(np.count_nonzero(np.array(total_area_bw_2fuel_sig == 0)))
            print("len(total_area_bw_2fuel_sig): ", len(total_area_bw_2fuel_sig))
            print("\n")
            print("\n")
            return total_area_bw_2fuel_sig, Straight_line_points


        def find_fuel_signal_consistency_frequency(potential_fuel_signal):
            to_check_days_threshold = 14
            fuel_signal_consistency_frequency = [0]*len(potential_fuel_signal)

            for i in range(len(potential_fuel_signal)):
                if potential_fuel_signal[i] == 1:
                    print("i: this i is in outer loop", i)

                    for j in range(1,to_check_days_threshold):
                        if potential_fuel_signal[i-j] == 1:
                            print("i: ", i)
                            print("j: ", j)
                            print("i-j: ", i-j)
                            print("fuel_signal_consistency_frequency[i-j]: ", fuel_signal_consistency_frequency[i-j])
                            fuel_signal_consistency_frequency[i] = fuel_signal_consistency_frequency[i-j]+1
                            print("New fuel_signal_consistency_frequency[i-j]: ", fuel_signal_consistency_frequency[i-j])
                            break

                        else:
                            fuel_signal_consistency_frequency[i] = 1

                else:
                    fuel_signal_consistency_frequency[i] = 0

            return fuel_signal_consistency_frequency



        def find_maximas(input_column):
            maximas_indices = argrelextrema(np.array(input_column), np.greater)[0]
            mean_of_maximas = np.mean(np.array(input_column)[maximas_indices])
            threshold = mean_of_maximas * 1.1
            for i in maximas_indices:
                if input_column[i] < threshold:
                    maximas_indices = np.delete(
                        maximas_indices, np.where(maximas_indices == i))

            potential_fuel_signal = [
                1 if i in maximas_indices else 0 for i in range(len(input_column))]

            return maximas_indices, potential_fuel_signal


        def find_overlapping_points(input_column1, input_column2):

        # first_non_nan_index = input_column1.first_valid_index()
            first_non_nan_index = 0
            for index, value in enumerate(input_column1):
                if not np.isnan(value):  # Replace 'None' with your specific non-value indicator
                    first_non_nan_index = index
                    break
        # print("first_non_nan_index: ", first_non_nan_index)
            overlapping_points = []
            input_column1_high = False

            if input_column1[0] > input_column2[0]:
                input_column1_high = True

            for i in range(first_non_nan_index, len(input_column1)):
                if input_column1_high == False:
                    if input_column1[i] >= input_column2[i]:
                        overlapping_points.append(i)
                        input_column1_high = True
                else:
                    if input_column1[i] <= input_column2[i]:
                        overlapping_points.append(i)
                        input_column1_high = False
        # print("overlapping_points: ", overlapping_points)
            return overlapping_points


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
            df.to_csv('output file with fuel elements.csv')

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

        def main2(df):

            df = df.dropna(subset=['Maverick', 'MaverickAlpha'])

        # Plot the original series
            Maverick = np.array(df['Maverick'])
            MaverickAlpha = np.array(df['MaverickAlpha'])
            columns_to_be_filtered = [Maverick, MaverickAlpha]
            fuel_status = calculate_fuel(Maverick, MaverickAlpha, 0, len(Maverick) - 1, False)
            df['fuel_status'] = fuel_status

            max_fuel_points, potential_fuel_signal = find_maximas(fuel_status)
            df['potential_fuel_signal'] = potential_fuel_signal

            print("max_fuel_points: ", max_fuel_points)
            max_fuel_values = np.take(fuel_status, max_fuel_points)

            total_area_bw_2fuel_sig, straight_line_points = area_between_fuel_potential_signals(fuel_status, max_fuel_points)
            fuel_signal_consistency_frequency = find_fuel_signal_consistency_frequency(potential_fuel_signal)
        # df['fuel_signal_consistency_frequency'] = fuel_signal_consistency_frequency

        # saving the dataframe to csv file
            df.to_csv('output file with fuel elements.csv')

        # Check if the last seven entries of the output_signal_max_fuel contain 1
            recieved = False
            if any(fuel_signal_consistency_frequency[-3:]):
                recieved = True

            fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, sharex=True)


            ax1.plot(df['Date'], Maverick, label='A')
            ax1.plot(df['Date'], MaverickAlpha, label='B')

        # filtered_points_wrt_angles,filtered_indices = finding_and_filtering_angles(df,columns_to_be_filtered)

        # ax1.scatter(maximas_indices, df.loc[maximas_indices, 'TopGunA'], c='green', label='local maximas')
        # ax1.scatter(minimas_indices, df.loc[minimas_indices, 'TopGunA'], c='red', label='local minimas')
            ax1.set_title('Msverick and MaverickAlpha')

        # ax1.scatter(sweet_spots_indices, df.loc[points_in_zone_indices, 'Sonar'], c='red', label='Sweet spots')
            ax1.legend()

        # ax3.plot(final_potential_points, label='potential_points also catering angle difference')

            print("fuel_status: ", fuel_status)

            ax2.plot(fuel_status, label='fuel_status')

            ax2.plot(range(max_fuel_points[0], max_fuel_points[-1] + 1), straight_line_points, label='straight_line_points')
            ax2.scatter(max_fuel_points, max_fuel_values, c='orange', label='fuel_signal')
            ax2.legend()

            ax3.plot(potential_fuel_signal, label="potential_fuel_signal")
            ax3.legend()

            print("Recieved:", recieved)

        # ax4.plot(range(max_fuel_points[0],(max_fuel_points[-1])),total_area_bw_2fuel_sig, label='total_area_between_orange_points')
            ax4.plot(df['Date'], fuel_signal_consistency_frequency, label='fuel_signal_consistency_frequency')
            ax4.legend()
            print("max_fuel_points: ", max_fuel_points)
            print("fuel_signal_consistency_frequency: ", fuel_signal_consistency_frequency)
        # saving the dataframe to csv file
            df.to_csv('output file with fuel elements.csv')
            plt.show()
            return recieved

        def main2_hourly(df):

            df = df.dropna(subset=['Maverick', 'MaverickAlpha'])

        # Plot the original series
            Maverick = np.array(df['Maverick'])
            MaverickAlpha = np.array(df['MaverickAlpha'])
            columns_to_be_filtered = [Maverick, MaverickAlpha]
            fuel_status = calculate_fuel(Maverick, MaverickAlpha, 0, len(Maverick) - 1, False)
            df['fuel_status'] = fuel_status

            max_fuel_points, potential_fuel_signal = find_maximas(fuel_status)
            df['potential_fuel_signal'] = potential_fuel_signal

            print("max_fuel_points: ", max_fuel_points)
            df['potential_fuel_signal'] = potential_fuel_signal
        # max_fuel_values = np.take(fuel_status, max_fuel_points)
        #
        # total_area_bw_2fuel_sig, straight_line_points = area_between_fuel_potential_signals(fuel_status, max_fuel_points)
        # fuel_signal_consistency_frequency = find_fuel_signal_consistency_frequency(potential_fuel_signal)
        #
        # df['fuel_signal_consistency_frequency'] = fuel_signal_consistency_frequency
        # # saving the dataframe to csv file


        # Check if the last seven entries of the output_signal_max_fuel contain 1
            recieved = False
            if any(potential_fuel_signal[-3:]):
                recieved = True

        # Set 'Date' column as index
        # df.set_index('Date', inplace=True)

        # fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)

        # fig, (ax1, ax3, ax4) = plt.subplots(3, 1, sharex=True)
        # ax1.plot(df['Date'], Maverick, label='A')
        # ax1.plot(df['Date'], MaverickAlpha, label='B')
        #
        # # filtered_points_wrt_angles,filtered_indices = finding_and_filtering_angles(df,columns_to_be_filtered)
        #
        # # ax1.scatter(maximas_indices, df.loc[maximas_indices, 'TopGunA'], c='green', label='local maximas')
        # # ax1.scatter(minimas_indices, df.loc[minimas_indices, 'TopGunA'], c='red', label='local minimas')
        # ax1.set_title('Msverick and MaverickAlpha')
        #
        #
        #
        # # ax1.scatter(sweet_spots_indices, df.loc[points_in_zone_indices, 'Sonar'], c='red', label='Sweet spots')
        # ax1.legend()
        #
        # # ax3.plot(final_potential_points, label='potential_points also catering angle difference')
        #
        # # df.reset_index(drop=True, inplace=True)  # Reset index starting from 0
        # # ax2.plot(df['fuel_status'], label='fuel_status')
        # #
        # # # ax2.scatter(max_fuel_points ,max_fuel_values,c='orange', label='fuel_signal')
        # # ax2.scatter(max_fuel_points, [df['fuel_status'][i]
        # #                               for i in max_fuel_points], c='red', label='sweet_fuel_signal')
        #
        # # Reset index for ax3 plot as well if needed
        #
        # total_area_bw_2fuel_sig, straight_line_points = area_between_fuel_potential_signals(
        # fuel_status, max_fuel_points)
        #
        # straight_line_points_array_for_completing_length_of_df = np.zeros(len(df))
        # straight_line_points_array_for_completing_length_of_df[df['potential_fuel_signal'].idxmax(
        # ):df['potential_fuel_signal'][::-1].idxmax() + 1] = straight_line_points
        # df['straight_line_points'] = straight_line_points_array_for_completing_length_of_df
        #
        # # ax2.plot(df['straight_line_points'],
        # #          label='straight_line_points', c='orange')
        #
        # # ax2.legend()
        # #
        # # ax2.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        #
        # df.reset_index(drop=True, inplace=True)  # Reset index starting from 0
        # ax3.plot(df['potential_fuel_signal'] , label="potential_fuel_signal")
        # ax3.legend()
        # ax3.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        #
        # print("Recieved:", recieved)
        # df.reset_index(drop=True, inplace=True)  # Reset index starting from 0
        # # ax4.plot(range(max_fuel_points[0],(max_fuel_points[-1])),total_area_bw_2fuel_sig, label='total_area_between_orange_points')
        # ax4.plot(df['fuel_signal_consistency_frequency'],
        # label='fuel_signal_consistency_frequency')
        #
        # ax4.legend()
        # ax4.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M:%S'))
        #
        # print("max_fuel_points: ", max_fuel_points)
        # print("fuel_signal_consistency_frequency: ", fuel_signal_consistency_frequency)
        #
        # plt.xticks(rotation=45, ha='right')
        # plt.xlim(df['Date'].min(), df['Date'].max())
        # plt.tight_layout()
        # plt.show()
            df.to_csv('output file with fuel elements.csv')
            return recieved

        def main3_hourly(df1):
        # df1 = pd.read_csv('output_2024-03-06_22-04-12.csv')
            print("********* here is the columns*********")
            print(df1.columns)
            print("********* here is the columns ends*********")
            print()
            print("********* here is the heads starts*********")
            print(df1.head())
            print("********* here is the heads ends*********")
            print("df starts")
        # dropping the rows with nan values in Maverick and MaverickAlpha
            fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1, sharex=True)
            df = df1.dropna(subset=['Maverick', 'MaverickAlpha'])
            ax1.plot(df['Date'], df['Maverick'], label='A')
            ax1.plot(df['Date'], df['MaverickAlpha'], label='B')
            ax1.set_title('Maverick and MaverickAlpha')
            ax1.legend()
            fuel_status = calculate_fuel(
                df['Maverick'], df['MaverickAlpha'], 0, len(df['Maverick'])-1, False)
            print('len of df maverick', len(df['Maverick']))
            print('len of fuel status', len(fuel_status))
            df['fuel_status'] = fuel_status
            print(len(fuel_status))
            print('len of df fuel', len(df['fuel_status']))
            print('len of actual df date', len(df.Date))
            max_fuel_points, potential_fuel_signal = find_maximas(fuel_status)
            print('printing max fuel points ', max_fuel_points)
            print('len of max_fuel ponts', len(max_fuel_points))
            df['potential_fuel_signal'] = potential_fuel_signal
        # calculating the area between the potential fuel signals and the straight line joining the max fuel points and the fuel status
            total_area_bw_2fuel_sig, straight_line_points = area_between_fuel_potential_signals(
                fuel_status, max_fuel_points)
            straight_line_points_array_for_completing_length_of_df = np.zeros(len(df))
            straight_line_points_array_for_completing_length_of_df[df['potential_fuel_signal'].idxmax(
            ):df['potential_fuel_signal'][::-1].idxmax()+1] = straight_line_points
            df['straight_line_points'] = straight_line_points_array_for_completing_length_of_df
        # completing an array and then putting the values og area between potential fuel points the straight line joining the max fuel points and the fuel status
            total_area_bw_2fuel_sig_array_for_completing_length_of_df = np.zeros(
                len(df))
            total_area_bw_2fuel_sig_array_for_completing_length_of_df[df['potential_fuel_signal'].idxmax(
            ):df['potential_fuel_signal'][::-1].idxmax()+1] = total_area_bw_2fuel_sig
            df['total_area_bw_2fuel_sig'] = total_area_bw_2fuel_sig_array_for_completing_length_of_df
        #    #  saving the output to a csv file
            df.to_csv('output file with fuel elements 5_5_2024.csv', index=False)
        # plotting the graph of ax2
            df.reset_index(drop=True, inplace=True)  # Reset index starting from 0
            ax2.plot(df['fuel_status'], label='fuel_status')
        # basically max_fuel_points are the points where the fuel status is maximum and the potential fuel signal is 1 so
        # we can replace it as max_fuel_points = df[df['potential fuel signal'] == 1].index
            ax2.scatter(df[df['potential_fuel_signal'] == 1].index, [df['fuel_status'][i]
                        for i in (df[df['potential_fuel_signal'] == 1].index)], c='red', label='sweet_fuel_signal')
            ax2.plot(df['straight_line_points'],
                    label='straight_line_points', c='orange')
            ax2.legend()
        # plotting ax3
            df.reset_index(drop=True, inplace=True)  # Reset index starting from 0
            ax3.plot(df['potential_fuel_signal'], label="potential_fuel_signal")
            ax3.legend()
            fuel_signal_consistency_frequency = find_fuel_signal_consistency_frequency(
                df['potential_fuel_signal'])
            df['fuel_signal_consistency_frequency'] = fuel_signal_consistency_frequency
        # plotting ax4
            ax4.plot(df['fuel_signal_consistency_frequency'],
                    label='fuel_signal_consistency_frequency')
            ax4.legend()
        # plotting ax5
            ax5.plot(df['total_area_bw_2fuel_sig'],
                    label='total_area_between_orange_straight_line and fuel_status')
        # ax4.plot(total_area_bw_2fuel_sig, label='total_area_between_orange_points')
        # print("max_fuel_points: ", max_fuel_points)
            ax5.legend()
            plt.show()
        import datetime
        import math
        import os

        import numpy as np
        import pandas as pd  # Added import for pandas
        from scipy import integrate
        from scipy.signal import argrelextrema
        import matplotlib.pyplot as plt
        from self import self
        pd.options.mode.copy_on_write = True
        # from dags.src.data.signals.Harpoon import Harpoon
        # from dags.src.data.signals.TopGun_Maverick import TopGun_Maverick
        # from dags.src.data.signals.deep_signals import harpoon_pink
        # from dags.src.data.utility import stockdata_helper_polygonio as stockdata_helper

        # from dags.src.data.signals import StochRSI, BollingerBands
        # from dags.src.data.signals.Sonar import Sonar
        # from dags.src.data.signals.TopGun_Maverick import TopGun_Maverick
        # from dags.src.data.signals.deep_signals.consolidate.deep_maverick import analyze_fuel_data
        # from dags.src.data.utility import stockdata_helper_polygonio, date_helper, leads_maverick_helper
        # from dags.src.data.utility.company_helper import fetch_all_companiesFortune1000
        # from dags.src.data.utility.leads_maverick_helper import LeadsMaverickHelper
        # from dags.src.orchestration.maverick_leads import update_leads_table
        # from dags.src.data.signals.primitive.counting_fuel_statuses import main

        import math
        from matplotlib import pyplot as plt
        import numpy as np
        import pandas as pd
        from scipy import integrate
        import matplotlib.dates
        from scipy.signal import argrelextrema
        from numpy.polynomial import polynomial as P
        import requests

        def find_slope_and_intercept(x2, y2, x1, y1):
            slope = (y2 - y1) / (x2 - x1)
            intercept = y1 - (slope * x1)
            return slope, intercept


        def find_area_between_two_lines(slope1, intercept1, slope2, intercept2, index):
        # a = 0  # Lower limit
        # b = 1  # Upper limit
        # result, error = integrate.quad(my_function, a, b)
            area = integrate.quad(lambda x: slope1 * x + intercept1 - slope2 * x - intercept2, index, index + 1)
            return area[0]


        def calculate_fuel(input_columnA, input_columnB, first_potential_fuel_index, last_potential_fuel_index,
                            first_point_check):
        # lets say input column1 is TopGunA (Blue) and input column2 is TopGunB (Orange)
            fuel = []
            length = len(input_columnA)
            for i, j in zip(range(first_potential_fuel_index, last_potential_fuel_index), range(len(input_columnB))):
        # print("i: ", i)
        # print("j: ", j)
        # print("input_columnB[i]: ", input_columnB[j])
        # print("input_columnB[i+1]: ", input_columnB[j+1])
                slope_of_A, intercept_of_A = find_slope_and_intercept(i + 1, input_columnA[i + 1], i, input_columnA[i])
                slope_of_B, intercept_of_B = find_slope_and_intercept(j + 1, input_columnB[j + 1], j, input_columnB[j])
                area = find_area_between_two_lines(slope_of_A, intercept_of_A, slope_of_B, intercept_of_B, i)

        # print("area", area)
                if i == 0 or first_point_check == True:
                    fuel.append(area)

                else:
                    fuel.append((-1 * area) + fuel[i - 1])

        # print("area: ", area)
        # print("fuel: ", fuel)

        # compensating for the last point as the last point is not calculated in the loop because one point is always left
        # for eample when calculting the area between 4 points we will have area 3 area points because the area is
        # calculated between the points, so the points are completed if compared to dataframe
            fuel.append(fuel[len(fuel) - 1])

            return fuel


        def area_between_fuel_potential_signals(fuel, orange_potential_fuel_signals):
            total_area_bw_2fuel_sig = []
            Straight_line_points = []
            first_point_check = True

            count = 0
            for i in range(len(orange_potential_fuel_signals) - 1):
                slope_of_orange_points, intercept_of_orange_points = find_slope_and_intercept(
                    orange_potential_fuel_signals[i + 1], fuel[orange_potential_fuel_signals[i + 1]],
                    orange_potential_fuel_signals[i], fuel[orange_potential_fuel_signals[i]])
                if count == 0:
                    for j in range(orange_potential_fuel_signals[i], orange_potential_fuel_signals[i + 1] + 1):
                        Straight_line_points.append(slope_of_orange_points * j + intercept_of_orange_points)
        # print('i',i)
        # print('J',j)
        # print('count',count+15)
        # print(fuel[j])
        # print(Straight_line_points[count])
        # print('check if value of straight line pont is same as value', Straight_line_points[count] == fuel[j])
        # print('straight_line_points', Straight_line_points)
                        count += 1
                else:
                    for j in range(orange_potential_fuel_signals[i] + 1, orange_potential_fuel_signals[i + 1] + 1):
                        Straight_line_points.append(slope_of_orange_points * j + intercept_of_orange_points)

        # print('i',i)
        # print('J',j)
        # print('count',count+15)
        # print(fuel[j])
        # print(Straight_line_points[count])
        # print('check if value of straight line pont is same as value', Straight_line_points[count] == fuel[j])
        # print('straight_line_points', Straight_line_points)
                        count += 1

                fuel_bw_2points = calculate_fuel(fuel, Straight_line_points, orange_potential_fuel_signals[i],
                                                orange_potential_fuel_signals[i + 1], first_point_check)

                fuel_bw_2points = [-x for x in fuel_bw_2points]

                total_area_bw_2fuel_sig += fuel_bw_2points

        # print("total_area_bw_2fuel_sig: ", total_area_bw_2fuel_sig)
        # print(np.count_nonzero(np.array(total_area_bw_2fuel_sig == 0)))
        # print("len(total_area_bw_2fuel_sig): ", len(total_area_bw_2fuel_sig))
        # print("\n")
        # print("\n")
            return total_area_bw_2fuel_sig, Straight_line_points


        def find_fuel_signal_consistency_frequency(potential_fuel_signal):
            to_check_days_threshold = 14
            fuel_signal_consistency_frequency = [0] * len(potential_fuel_signal)

            for i in range(len(potential_fuel_signal)):
                if potential_fuel_signal[i] == 1:
        # print("i: this i is in outer loop", i)

                    for j in range(1, to_check_days_threshold):
                        if potential_fuel_signal[i - j] == 1:
        # print("i: ", i)
        # print("j: ", j)
        # print("i-j: ", i - j)
        # print("fuel_signal_consistency_frequency[i-j]: ", fuel_signal_consistency_frequency[i - j])
                            fuel_signal_consistency_frequency[i] = fuel_signal_consistency_frequency[i - j] + 1
        # print("New fuel_signal_consistency_frequency[i-j]: ", fuel_signal_consistency_frequency[i - j])
                            break

                        else:
                            fuel_signal_consistency_frequency[i] = 1

                else:
                    fuel_signal_consistency_frequency[i] = 0

            return fuel_signal_consistency_frequency


        def find_maximas(input_column):
            maximas_indices = argrelextrema(np.array(input_column), np.greater)[0]
            mean_of_maximas = np.mean(np.array(input_column)[maximas_indices])
            threshold = mean_of_maximas * 1.1
            for i in maximas_indices:
                if input_column[i] < threshold:
                    maximas_indices = np.delete(maximas_indices, np.where(maximas_indices == i))

            potential_fuel_signal = [1 if i in maximas_indices else 0 for i in range(len(input_column))]

            return maximas_indices, potential_fuel_signal


        def find_overlapping_points(input_column1, input_column2):
        # first_non_nan_index = input_column1.first_valid_index()
            first_non_nan_index = 0
            for index, value in enumerate(input_column1):
                if not np.isnan(value):  # Replace 'None' with your specific non-value indicator
                    first_non_nan_index = index
                    break
        # print("first_non_nan_index: ", first_non_nan_index)
            overlapping_points = []
            input_column1_high = False

            if input_column1[0] > input_column2[0]:
                input_column1_high = True

            for i in range(first_non_nan_index, len(input_column1)):
                if input_column1_high == False:
                    if input_column1[i] >= input_column2[i]:
                        overlapping_points.append(i)
                        input_column1_high = True
                else:
                    if input_column1[i] <= input_column2[i]:
                        overlapping_points.append(i)
                        input_column1_high = False
            print("overlapping_points: ", overlapping_points)
            return overlapping_points

        import requests
        import datetime
        # enday=datetime.date.today().strftime('%Y-%m-%d')
        # current_date = datetime.now().strftime("%Y-%m-%d")
        def load_data_from_api(st_name,enday):

            url = "https://stapi02.azurewebsites.net/api/httpstsignals"
            params = {
                "code": "TryM8ecL_3NA8n8CtLwgowLvm08BAHpC3Xp4_QwxtqTKAzFugvz0LQ==",
                "name": st_name,
                "start_date": "2020-01-01",
                "end_date": enday
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Raises HTTPError for bad responses (4xx and 5xx)
                data = response.json()
                data = pd.DataFrame(data)
                data['Date'] = pd.to_datetime(data['Date'])
                data.set_index('Date', inplace=True)
                return data
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                print(f"Request error occurred: {req_err}")
            except Exception as e:
                print(f"An error occurred: {e}")
            return None


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
                    recieved = main(df)


        # print("Recieved:", recieved)


        # plt.show()D
                    if recieved ==True:
        # print(stock_name)
                        helper = LeadsMaverickHelper()
                        helper.insert_lead(stock_name, end_date)

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

        def main1():
            start_date = datetime.datetime(2022, 11, 1)
            end_date = datetime.datetime(2024, 1, 9)
        # stock_name = "X:BTCUSD"
            stock_name = "FNMA"

            df = TopGun_Maverick.setup_specific(self, stock_name, start_date, end_date)
            fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, sharex=True)
            df = df.dropna(subset=['Maverick', 'MaverickAlpha'])

        # Plot the original series
            Maverick = np.array(df['Maverick'])
            MaverickAlpha = np.array(df['MaverickAlpha'])
            columns_to_be_filtered = [Maverick, MaverickAlpha]
            ax1.plot(df['Date'], Maverick, label='A')
            ax1.plot(df['Date'], MaverickAlpha, label='B')

        # filtered_points_wrt_angles,filtered_indices = finding_and_filtering_angles(df,columns_to_be_filtered)

        # ax1.scatter(maximas_indices, df.loc[maximas_indices, 'TopGunA'], c='green', label='local maximas')
        # ax1.scatter(minimas_indices, df.loc[minimas_indices, 'TopGunA'], c='red', label='local minimas')
            ax1.set_title('Msverick and MaverickAlpha')

        # ax1.scatter(sweet_spots_indices, df.loc[points_in_zone_indices, 'Sonar'], c='red', label='Sweet spots')
            ax1.legend()

        # ax3.plot(final_potential_points, label='potential_points also catering angle difference')
            fuel_status = calculate_fuel(Maverick, MaverickAlpha, 0, len(Maverick) - 1, False)
            df['fuel_status'] = fuel_status

            max_fuel_points, potential_fuel_signal = find_maximas(fuel_status)
            df['potential_fuel_signal'] = potential_fuel_signal

        # print("max_fuel_points: ", max_fuel_points)

        # print("fuel_status: ", fuel_status)

            ax2.plot(fuel_status, label='fuel_status')

            max_fuel_values = np.take(fuel_status, max_fuel_points)

            total_area_bw_2fuel_sig, straight_line_points = area_between_fuel_potential_signals(fuel_status, max_fuel_points)
            ax2.plot(range(max_fuel_points[0], max_fuel_points[-1] + 1), straight_line_points, label='straight_line_points')
            ax2.scatter(max_fuel_points, max_fuel_values, c='orange', label='fuel_signal')
            ax2.legend()

            ax3.plot(potential_fuel_signal, label="potential_fuel_signal")
            ax3.legend()

            fuel_signal_consistency_frequency = find_fuel_signal_consistency_frequency(potential_fuel_signal)
            df['fuel_signal_consistency_frequency'] = fuel_signal_consistency_frequency

        # saving the dataframe to csv file
        # df.to_csv('output file with fuel elements.csv')

        # Check if the last seven entries of the output_signal_max_fuel contain 1
            recieved = False
            if any(fuel_signal_consistency_frequency[-3:]):
                recieved = True

            print("Recieved:", recieved)

        # ax4.plot(range(max_fuel_points[0],(max_fuel_points[-1])),total_area_bw_2fuel_sig, label='total_area_between_orange_points')
            ax4.plot(df['Date'], fuel_signal_consistency_frequency, label='fuel_signal_consistency_frequency')
            ax4.legend()
        # print("max_fuel_points: ", max_fuel_points)
        # print("fuel_signal_consistency_frequency: ", fuel_signal_consistency_frequency)
        # plt.show()


        def find_local_maxima(values, window_size):
            local_maxima = []
            for i in range(len(values)):
                window_start = max(0, i - window_size)
                window_end = min(len(values) - 1, i + window_size)
                if values[i] == max(values[window_start:window_end + 1]):
                    local_maxima.append(i)
            return local_maxima


        def insert_leads(start_date, end_date):
            # Define connection parameters
            db_url = 'postgresql://stpostgres:stocktrader@sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com:5432/postgres'

            # Create the SQLAlchemy engine
            engine = create_engine(db_url)

            """
            Inserts data from `stocktrader.leads_1` into `stocktrader.leads` based on the given date range.

            Args:
                start_date (datetime): The start date as a datetime object.
                end_date (datetime): The end date as a datetime object.
            """
            # Define the query with placeholders for dates
            query = """
            INSERT INTO stocktrader.leads (stock_name, lead_date, sealing_date, sealing_flag, sealing_price, lead_price, active_flag, endorsement)
            SELECT stock_name, lead_date, sealing_date, sealing_flag, sealing_price, lead_price, active_flag, 'Yes'
            FROM stocktrader.leads_1
            WHERE
                lead_date >= :start_date
                AND lead_date <= :end_date
            ON CONFLICT (stock_name, lead_date)
            DO UPDATE SET
                sealing_date = EXCLUDED.sealing_date,
                sealing_flag = EXCLUDED.sealing_flag,
                sealing_price = EXCLUDED.sealing_price,
                lead_price = EXCLUDED.lead_price,
                active_flag = EXCLUDED.active_flag,
                endorsement = 'Yes';
            """

            try:
                with engine.connect() as connection:
                    # Debugging: Print the query and parameters
                    print(f"Executing Query:\n{query}")
                    print(f"With Parameters: start_date = {start_date}, end_date = {end_date}")

                    # Execute the query with parameters
                    result = connection.execute(text(query), {"start_date": start_date, "end_date": end_date})

                    # Manually commit the transaction
                    connection.commit()

                    # Check how many rows were affected by the query
                    if result.rowcount > 0:
                        print(f"Rows inserted/updated: {result.rowcount}")
                    else:
                        print("No rows were inserted or updated.")

            except Exception as e:
                print(f"An error occurred: {e}")



        
        st = datetime.datetime.now()
        # st = datetime.datetime(2025, 4, 11)
        en = datetime.datetime.now()
        # en = datetime.datetime(2025, 3, 10)
        orchestrate_leads(st,en)
        # insert_leads(st,en)
        connect()

    @task()
    def valid_signal_dag():
      import datetime
      import pandas as pd
      import matplotlib.pyplot as plt
      import requests
      import numpy as np
      from scipy.signal import argrelextrema, savgol_filter

      # Function to load data from the API
      def load_data_from_api(stock_name, start_date, end_date):
          url = "https://stapi02.azurewebsites.net/api/httpstsignals"
          params = {
              "code": "TryM8ecL_3NA8n8CtLwgowLvm08BAHpC3Xp4_QwxtqTKAzFugvz0LQ==",
              "name": stock_name,
              "start_date": start_date.strftime("%Y-%m-%d"),
              "end_date": end_date.strftime("%Y-%m-%d")
          }
          response = requests.get(url, params=params)
          if response.status_code == 200:
              data = response.json()
              df = pd.DataFrame(data)
              return df
          else:
              print(f"Error fetching data: {response.status_code}")
              return pd.DataFrame()

      # Function to categorize activity levels
      def categorize_activity(pct_change):
          if pct_change <= -2:
              return 'Highly Dormant'
          elif -2 < pct_change <= 0:
              return 'Dormant'
          elif 0 < pct_change <= 2:
              return 'Moderately Active'
          elif 2 < pct_change <= 5:
              return 'Active'
          else:
              return 'Highly Active'


      import psycopg2
      import pandas as pd
      from sqlalchemy import create_engine, text
      # Database connection details
      db_config = {
          'host': 'sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com',
          'database': 'postgres',
          'user': 'stpostgres',
          'password': 'stocktrader'
      }

      try:
          # Connect to PostgreSQL
          conn = psycopg2.connect(**db_config)
          cursor = conn.cursor()

          # Fetch all data from 'lead' table
          cursor.execute("SELECT * FROM stocktrader.leads_1 WHERE lead_date >= '2025-03-01' AND lead_date <= '2025-12-31';")## 2021-02-02
          data = cursor.fetchall()

          # Column names from cursor description
          columns = [desc[0] for desc in cursor.description]

          # Convert to DataFrame
          dfff = pd.DataFrame(data, columns=columns)
          # dfff=dfff[4838:]
          # dfff.reset_index(inplace=True)
          # Print DataFrame
          # print(df)

          # Close connection
          cursor.close()
          conn.close()

      except Exception as e:
          print("Error:", e)


      def update_signal_price(signal, stock_name, lead_date, lead_id):
          try:
              # Connect to PostgreSQL
              conn = psycopg2.connect(**db_config)
              cursor = conn.cursor()

              # Update Query
              update_query = """
              UPDATE stocktrader.leads_1
              SET valid = %s
              WHERE stock_name = %s AND lead_date = %s AND id = %s;
              """

              # Execute Query
              cursor.execute(update_query, (signal, stock_name, lead_date, lead_id))
              conn.commit()  # Save changes

              print("Siganl updated successfully.")

              # Close connection
              cursor.close()
              conn.close()

          except Exception as e:
              print("Error:", e)

      import pandas as pd

      def check_date_before(df, date_column, given_date):
          """
          Checks if there exists a date in the dataframe that is exactly 5 days before the given date.

          :param df: Pandas DataFrame containing a date column
          :param date_column: Name of the date column in the DataFrame
          :param given_date: The reference date to check against (string or datetime)
          :return: True if a date exists that is exactly 5 days before, otherwise False
          """
          # Convert given_date to datetime
          given_date = pd.to_datetime(given_date)

          # Calculate the target date (5 days before)
          target_date = given_date - pd.Timedelta(days=7)

          # Convert the date column to datetime (if not already)
          df[date_column] = pd.to_datetime(df[date_column])

          # Check if the target_date exists in the DataFrame
          return target_date in df[date_column].values

      # Example usage
      # Assuming check_df has a column named 'date'
      # result = check_date_before(check_df, 'date', '2025-02-10')
      # print(result)  # True or False
      print(len(dfff))
      for i in range (len(dfff)):
        print(len(dfff))
        print(i)
        stk=dfff.loc[i]['stock_name']
        print(stk)
        st=dfff.loc[i]['lead_date']
        id=dfff.loc[i]['id']
        
        start_date = datetime.datetime(2020, 1, 1)
        end_date = datetime.datetime(2025, 2, 9)
        stock_name = stk

        df = load_data_from_api(stock_name, start_date, end_date)

        if df.empty:
            print("No data was loaded. Exiting program.")
            exit()

        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        weekly_df = df.resample('W-MON').last()
        weekly_df['Pct_Change'] = weekly_df['Close'].pct_change() * 100
        weekly_df['Activity_Level'] = weekly_df['Pct_Change'].apply(categorize_activity)

        weekly_df['EMA_1'] = weekly_df['Pct_Change'].ewm(span=1, adjust=False).mean()
        weekly_df['EMA_3'] = weekly_df['Pct_Change'].ewm(span=3, adjust=False).mean()
        weekly_df['EMA_5'] = weekly_df['Pct_Change'].ewm(span=5, adjust=False).mean()

        # Fix: Ensure no missing values before rolling sum calculation
        weekly_df['Momentum_Change'] = weekly_df['Activity_Level'].apply(
            lambda x: 1 if x in ['Moderately Active', 'Active', 'Highly Active'] else -1
        )
        weekly_df['Momentum_Change'].fillna(0, inplace=True)  # Fill any NaN values

        # Compute the rolling momentum score
        weekly_df['Momentum_Score'] = weekly_df['Momentum_Change'].rolling(window=21, min_periods=1).sum()

        # Apply Savitzky-Golay filter to smooth the Momentum Score
        weekly_df['Smoothed_Momentum_Score'] = savgol_filter(weekly_df['Momentum_Score'], window_length=21, polyorder=3)

        # Find local minima and maxima on the smoothed momentum score
        window = 10
        weekly_df['Smoothed_Minima'] = weekly_df['Smoothed_Momentum_Score'].iloc[argrelextrema(
            weekly_df['Smoothed_Momentum_Score'].values, np.less_equal, order=window)[0]]
        weekly_df['Smoothed_Maxima'] = weekly_df['Smoothed_Momentum_Score'].iloc[argrelextrema(
            weekly_df['Smoothed_Momentum_Score'].values, np.greater_equal, order=window)[0]]

        # Define uptrend between minima and maxima
        weekly_df['Uptrend'] = 0
        in_uptrend = False
        for i in range(1, len(weekly_df)):
            if not in_uptrend and not np.isnan(weekly_df['Smoothed_Minima'].iloc[i]):
                in_uptrend = True
            if in_uptrend and not np.isnan(weekly_df['Smoothed_Maxima'].iloc[i]):
                in_uptrend = False
            weekly_df.loc[weekly_df.index[i], 'Uptrend'] = int(in_uptrend)

        # --- Wind Signal Activation ---
        weekly_df['Wind'] = 0
        wind_active = False
        for i in range(1, len(weekly_df)):
            if not wind_active and weekly_df['Pct_Change'].iloc[i - 1] < -1 and weekly_df['Pct_Change'].iloc[i] > 0:
                wind_active = True
            if wind_active and weekly_df['EMA_1'].iloc[i] < weekly_df['EMA_3'].iloc[i]:
                wind_active = False
            weekly_df.loc[weekly_df.index[i], 'Wind'] = int(wind_active)

        # Save the DataFrame to CSV
        csv_filename = "AAPL_weekly_analysis.csv"
        weekly_df.to_csv(csv_filename)
        print(f"✅ DataFrame saved to {csv_filename}")

        # Ensure Momentum_Score exists before plotting
        print("Columns in weekly_df:", weekly_df.columns)

        weekly_df.reset_index(inplace=True)
        check_df=weekly_df[weekly_df['Uptrend']==1]
        result = check_date_before(check_df, 'Date', st)

        if result==True:

            update_signal_price('Yes',stk,st,int(id))
        else:
            update_signal_price('No',stk,st,int(id))

    @task()
    def price_dag():
      import datetime
      import pandas as pd
      import matplotlib.pyplot as plt
      import requests
      import numpy as np
      from scipy.signal import argrelextrema, savgol_filter
      import psycopg2


      from scipy.signal import savgol_filter, find_peaks, argrelextrema



      def load_data_from_api(stock_name, start_date, end_date):
          # ✅ Convert string dates to datetime objects
          if isinstance(start_date, str):
              start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
          if isinstance(end_date, str):
              end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

          url = "https://stapi02.azurewebsites.net/api/httpstsignals"
          params = {
              "code": "TryM8ecL_3NA8n8CtLwgowLvm08BAHpC3Xp4_QwxtqTKAzFugvz0LQ==",
              "name": stock_name,
              "start_date": start_date.strftime("%Y-%m-%d"),
              "end_date": end_date.strftime("%Y-%m-%d")
          }
          response = requests.get(url, params=params)
          if response.status_code == 200:
              data = response.json()
              df = pd.DataFrame(data)
              return df
          else:
              print(f"Error fetching data: {response.status_code}")
              return pd.DataFrame()



      import pandas as pd
      from sqlalchemy import create_engine, text
      # Database connection details
      db_config = {
          'host': 'sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com',
          'database': 'postgres',
          'user': 'stpostgres',
          'password': 'stocktrader'
      }

      try:
          # Connect to PostgreSQL
          conn = psycopg2.connect(**db_config)
          cursor = conn.cursor()

          # # Fetch all data from 'lead' table
          cursor.execute("SELECT * FROM stocktrader.leads_1 WHERE lead_date >= '2025-01-01' AND lead_date <= '2025-12-31' AND valid = 'Yes' ORDER BY lead_date, stock_name;")
          # cursor.execute("SELECT * FROM stocktrader.leads_1 WHERE lead_date IN ('2020-09-07','2021-04-05','2020-05-25','2021-01-18','2020-11-26','2021-02-15');")
          data = cursor.fetchall()

          # Column names from cursor description
          columns = [desc[0] for desc in cursor.description]

          # Convert to DataFrame
          dfff = pd.DataFrame(data, columns=columns)
          # dfff=dfff[5307:]
          # dfff.reset_index(inplace=True)
          # Print DataFrame
          # print(df)

          # Close connection
          cursor.close()
          conn.close()

      except Exception as e:
          print("Error:", e)


      def update_lead_price(lead_price, stock_name, lead_date, lead_id):
          try:
              # Connect to PostgreSQL
              conn = psycopg2.connect(**db_config)
              cursor = conn.cursor()

              # Update Query
              update_query = """
              UPDATE stocktrader.leads_1
              SET lead_date_price = %s
              WHERE stock_name = %s AND lead_date = %s AND id = %s;
              """

              # Execute Query
              cursor.execute(update_query, (lead_price, stock_name, lead_date, lead_id))
              conn.commit()  # Save changes

              print("lead_price updated successfully.")

              # Close connection
              cursor.close()
              conn.close()

          except Exception as e:
              print("Error:", e)



      def update_sell_price(lead_price, stock_name, lead_date, lead_id):
          try:
              # Connect to PostgreSQL
              conn = psycopg2.connect(**db_config)
              cursor = conn.cursor()

              # Update Query
              update_query = """
              UPDATE stocktrader.leads_1
              SET sell_signal_price = %s
              WHERE stock_name = %s AND lead_date = %s AND id = %s;
              """

              # Execute Query
              cursor.execute(update_query, (lead_price, stock_name, lead_date, lead_id))
              conn.commit()  # Save changes

              print("sell_signal_price updated successfully.")

              # Close connection
              cursor.close()
              conn.close()

          except Exception as e:
              print("Error:", e)




      def is_friday(st):
          day = datetime.datetime.strptime(st, "%Y-%m-%d").strftime("%A")
          return "Yes" if day == "Friday" else "No"


      def update_sell_signal_date(sell_date, stock_name, lead_date, lead_id):
          try:
              # Connect to PostgreSQL
              conn = psycopg2.connect(**db_config)
              cursor = conn.cursor()

              # Update Query
              update_query = """
              UPDATE stocktrader.leads_1
              SET sell_signal_date = %s
              WHERE stock_name = %s AND lead_date = %s AND id = %s;
              """

              # Execute Query
              cursor.execute(update_query, (sell_date, stock_name, lead_date, lead_id))
              conn.commit()  # Save changes

              print("Sell signal date updated successfully.")

              # Close connection
              cursor.close()
              conn.close()

          except Exception as e:
              print("Error:", e)






      print(len(dfff))

      for i in range (len(dfff)):

        # display(df.loc[i:])
        # display(df.loc[i]['stock_name'])
        # display(df.loc[i]['lead_date'])
        stk=dfff.loc[i]['stock_name']
        st=dfff.loc[i]['lead_date']
        fixed_st=dfff.loc[i]['lead_date']
        id=dfff.loc[i]['id']
        print(i)
        print(stk,st,id)
        # stk=dfff.loc[i]['stock_name']
        # st=sell_signal_date
        # st='2020-03-12'
        # lt=dfff.loc[i]['lead_date']

        if isinstance(st, datetime.date):
          st_date = st  # Already a date, no conversion needed
        else:
            st_date = datetime.datetime.strptime(st, "%Y-%m-%d").date()
        en_date = st_date + datetime.timedelta(days=10)
        st_date=st_date - datetime.timedelta(days=10)

        # ✅ Convert string to datetime and add one day
        # st_date = datetime.datetime.strptime(st, "%Y-%m-%d")
        # en_date = st_date + datetime.timedelta(days=1)

        # ✅ Convert back to string format
        en = en_date.strftime("%Y-%m-%d")
        stt=st_date.strftime("%Y-%m-%d")
        stp=st.strftime("%Y-%m-%d")

        # ✅ Function Call with Corrected Dates
        df = load_data_from_api(stk, stt, en)

        if df is not None and not df.empty:
          if not df[df['Date'] == stp].empty:
            ld_p=df[df['Date']==stp]['Open'].values[0]
            print(ld_p)
            update_lead_price(float(ld_p),stk,st,int(id))
          else:
            if isinstance(st, datetime.date):
              st_date = st  # Already a date, no conversion needed
            else:
                st_date = datetime.datetime.strptime(st, "%Y-%m-%d").date()   ### ager friday ka data nhi ho ga to monday ki price append ho gi
            st_date=st_date + datetime.timedelta(days=3)
            res=is_friday(stp)
            if res == "Yes":
              stp=st_date.strftime("%Y-%m-%d")
              if not df[df['Date'] == stp].empty:
                ld_p=df[df['Date']==stp]['Close'].values[0]
                print(ld_p)
                update_lead_price(float(ld_p),stk,st,int(id))
            else:
              df['Date'] = pd.to_datetime(df['Date'])
              stp = pd.to_datetime(stp)  # Convert to datetime

              # Filter for same or next available date
              filtered_dates = df[df['Date'] >= stp]

              # Update stp with the first available date after or equal to 'stp'
              if not filtered_dates.empty:
                  stp = filtered_dates['Date'].iloc[0]  # First available date

              stp = stp.strftime("%Y-%m-%d")
              if not df[df['Date'] == stp].empty:
                ld_p=df[df['Date']==stp]['Close'].values[0]
                print(ld_p)
                update_lead_price(float(ld_p),stk,st,int(id))



        def round_data(data, column_name):
            return data[column_name].round(3)

        # Function to smooth data using Savitzky-Golay filter
        def smooth_data(y_values, window_size=11, polyorder=3):
            if window_size % 2 == 0:
                window_size += 1  # window_size must be odd
            return savgol_filter(y_values, window_size, polyorder)

        # def smooth_data(y_values, polyorder=3):
        #     window_size = min(11, len(y_values) // 2 * 2 + 1)  # Make sure window_size is odd and fits the data
        #     return savgol_filter(y_values, window_size, polyorder)


        # Function to compute derivative of smoothed data
        def compute_derivative(smoothed_data, x_numerical):
            return np.gradient(smoothed_data, x_numerical).round(3)

        # Function to find and refine peaks (local maxima) and troughs (local minima)
        def find_extrema(smoothed_data, height=0.02, distance=10):
            peaks, _ = find_peaks(smoothed_data, height=height, distance=distance)
            troughs = argrelextrema(smoothed_data, np.less_equal, order=distance)[0]
            return peaks, troughs

        def find_missing_dates(data):
            if data.index.name != 'Date':
                raise ValueError("The index is not set to 'Date'")

            start_date = data.index.min()
            end_date = data.index.max()
            full_date_range = pd.date_range(start=start_date, end=end_date, freq='D')

            existing_dates = data.index
            missing_dates = full_date_range.difference(existing_dates)

            holiday_df = pd.DataFrame({'Missing Date': missing_dates})
            return holiday_df

        # Function to subdivide date ranges excluding missing dates
        def subdivide_ranges_excluding_missing(region_df, missing_dates_df):
            # Convert columns to datetime
            region_df['Start Date'] = pd.to_datetime(region_df['Start Date'])
            region_df['End Date'] = pd.to_datetime(region_df['End Date'])
            missing_dates = pd.to_datetime(missing_dates_df['Missing Date'])

            # Initialize a list to collect subdivided ranges
            subdivided_ranges = []

            for _, row in region_df.iterrows():
                start_date = row['Start Date']
                end_date = row['End Date']

                # Collect all missing dates within the current range
                dates_within_range = missing_dates[(missing_dates >= start_date) & (missing_dates <= end_date)]

                if dates_within_range.empty:
                    # No missing dates within the range, add the original range
                    subdivided_ranges.append(row)
                else:
                    # There are missing dates, subdivide the range
                    previous_end = start_date
                    for missing_date in dates_within_range:
                        if previous_end < missing_date:
                            subdivided_ranges.append({
                                'Region Type': row['Region Type'],
                                'Start Date': previous_end,
                                'End Date': missing_date - pd.Timedelta(days=1)
                            })
                        previous_end = missing_date + pd.Timedelta(days=1)

                    # Add the final segment after the last missing date, if any
                    if previous_end <= end_date:
                        subdivided_ranges.append({
                            'Region Type': row['Region Type'],
                            'Start Date': previous_end,
                            'End Date': end_date
                        })

            # Convert list of dicts to DataFrame
            subdivided_df = pd.DataFrame(subdivided_ranges)

            return subdivided_df
        def load_data_from_api2(st_name):
            enday=datetime.date.today().strftime('%Y-%m-%d')
            url = "https://stapi02.azurewebsites.net/api/httpstsignals"
            params = {
                "code": "TryM8ecL_3NA8n8CtLwgowLvm08BAHpC3Xp4_QwxtqTKAzFugvz0LQ==",
                "name": st_name,
                "start_date": "2020-01-01",
                "end_date": enday
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Raises HTTPError for bad responses (4xx and 5xx)
                data = response.json()
                data = pd.DataFrame(data)
                data['Date'] = pd.to_datetime(data['Date'])
                data.set_index('Date', inplace=True)
                return data
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                print(f"Request error occurred: {req_err}")
            except Exception as e:
                print(f"An error occurred: {e}")
            return None

        st_name=stk

        data = load_data_from_api2(st_name)
        if data is not None:
            print("Data loaded successfully!")
            data['H9'] = round_data(data, 'H9')
            data['H14'] = round_data(data, 'H14')

            x_dates = data.index  # This is a DatetimeIndex now

            smoothed_H9 = smooth_data(data['H9'])
            smoothed_H14 = smooth_data(data['H14'])

            df_dx_smoothed_H9 = compute_derivative(smoothed_H9, np.arange(len(data)))
            df_dx_smoothed_H14 = compute_derivative(smoothed_H14, np.arange(len(data)))

            # plt.figure(figsize=(12, 6))

            # Find and pass missing dates
            missing_dates_df = find_missing_dates(data)

            # Plot and collect region data for H9
            # region_dfs_h9 = plot_data(x_dates, data['H9'], smoothed_H9, df_dx_smoothed_H9, 'H9', show_extrema=True, plot_regions_func=plot_regions)

            # Subdivide the regions excluding missing dates for each color
            # filtered_region_dfs_h9 = {color: subdivide_ranges_excluding_missing(df, missing_dates_df) for color, df in region_dfs_h9.items()}

            # Function to collect green and red dots into a DataFrame
            def collect_green_red_dots(smoothed_data=smoothed_H9, x_dates=x_dates):
                peaks, troughs = find_extrema(smoothed_data)

                # Create a DataFrame for green (peaks) dots
                green_dots_df = pd.DataFrame({
                    'Date': x_dates[peaks],
                    'Color': 'green'
                })

                # Create a DataFrame for red (troughs) dots
                red_dots_df = pd.DataFrame({
                    'Date': x_dates[troughs],
                    'Color': 'red'
                })

                # Concatenate the two DataFrames
                dots_df = pd.concat([green_dots_df, red_dots_df]).sort_values(by='Date').reset_index(drop=True)

                return dots_df,data

        # Example usage
        dots_df,df = collect_green_red_dots()

        df_reset = df.reset_index(inplace= True)
        data=df.copy()
        df=df[['Date','Close']]

        buy_signals=dots_df[dots_df['Color']=='red']['Date']
        sell_signals=dots_df[dots_df['Color']=='green']['Date']

        df=data.copy()
        df['Date'] = pd.to_datetime(df['Date'])
        # df['Lead'] = df['Date'].isin(pd.to_datetime(lead_dates))
        df['Buy'] = df['Date'].isin(pd.to_datetime(buy_signals))
        df['Sell'] = df['Date'].isin(pd.to_datetime(sell_signals))

        def check_sell(start_date, end_date, data=dots_df):
            # Convert dates to datetime objects
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)

            # Define the range to check: 2 days before the end_date
            check_start = end_date - pd.Timedelta(days=2)
            check_end = end_date + pd.Timedelta(days=50)
            # Ensure check_start is not before start_date
            check_start = max(check_start, start_date)

            # Filter data within the range
            mask = (data['Date'] >= check_start) & (data['Date'] <= check_end)
            relevant_data = data[mask]

            # Check for 'green' color
            green_dates = relevant_data[relevant_data['Color'] == 'green']

            if not green_dates.empty:
                # dd=f"Sell opportunity detected on: {green_dates['Date'].dt.date.tolist()[-1]}"
                dd={green_dates['Date'].dt.date.tolist()[0]}
                return True, dd #green_dates['Date'].dt.date.tolist()  # Return True and the list of dates with green color
            else:
                # dd="No sell opportunity found."
                dd=None
                return False, dd
            # # # Convert dates to datetime objects
            # # start_date = pd.to_datetime(start_date)
            # # end_date = pd.to_datetime(end_date)

            # # # Define the range to check: 2 days before and 2 days after end_date
            # # check_start = end_date - pd.Timedelta(days=2)
            # # check_end = end_date + pd.Timedelta(days=5)

            # # # Ensure check_start is not before start_date
            # # check_start = max(check_start, start_date)

            # # # Filter data within the range
            # # mask = (data['Date'] >= check_start) & (data['Date'] <= check_end)
            # # relevant_data = data[mask]

            # # # Check for 'green' color
            # # green_dates = relevant_data[relevant_data['Color'] == 'green']

            # # if not green_dates.empty:
            # #     dd = f"{tick} Sell opportunity detected on: {green_dates['Date'].dt.date.tolist()}"
            # #     return True, dd
            # # else:
            # #     dd = f"{tick} No sell opportunity found."
            # #     return False, dd


            # # Convert dates to datetime objects
            # start_date = pd.to_datetime(start_date)
            # end_date = pd.to_datetime(end_date)

            # # Define the range to check: 2 days before the end_date
            # check_start = end_date - pd.Timedelta(days=3)

            # # Ensure check_start is not before start_date
            # check_start = max(check_start, start_date)

            # # Filter data within the range
            # mask = (data['Date'] >= check_start) & (data['Date'] <= end_date)
            # relevant_data = data[mask]

            # # Check for 'green' color
            # green_dates = relevant_data[relevant_data['Color'] == 'green']

            # if not green_dates.empty:
            #     dd=f"Sell opportunity detected on: {green_dates['Date'].dt.date.tolist()}"
            #     return True, dd #green_dates['Date'].dt.date.tolist()  # Return True and the list of dates with green color
            # else:
            #     dd="No sell opportunity found."
            #     return False, dd

        def check_buy(start_date, end_date, data=dots_df):
            # # Convert dates to datetime objects
            # start_date = pd.to_datetime(start_date)
            # end_date = pd.to_datetime(end_date)

            # # Define the range to check: 2 days before and 2 days after end_date
            # check_start = end_date - pd.Timedelta(days=2)
            # check_end = end_date + pd.Timedelta(days=5)

            # # Ensure check_start is not before start_date
            # check_start = max(check_start, start_date)

            # # Filter data within the range
            # mask = (data['Date'] >= check_start) & (data['Date'] <= check_end)
            # relevant_data = data[mask]

            # # Check for 'red' color
            # red_dates = relevant_data[relevant_data['Color'] == 'red']

            # if not red_dates.empty:
            #     dd = f"{tick} Buy opportunity detected on: {red_dates['Date'].dt.date.tolist()}"
            #     return True, dd
            # else:
            #     dd = f"{tick} No buy opportunity found."
            #     return False, dd


            # Convert dates to datetime objects
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)

            # Define the range to check: 2 days before the end_date
            check_start = end_date - pd.Timedelta(days=5)

            # Ensure check_start is not before start_date
            check_start = max(check_start, start_date)

            # Filter data within the range
            mask = (data['Date'] >= check_start) & (data['Date'] <= end_date)
            relevant_data = data[mask]

            # Check for 'red' color
            red_dates = relevant_data[relevant_data['Color'] == 'red']

            if not red_dates.empty:
                dd=f"Buy opportunity detected on: {red_dates['Date'].dt.date.tolist()}"
                return True,dd  # Return True and the list of dates with red color
            else:
                dd="No buy opportunity found."
                return False, dd

        start_date="2020-01-01"
        # end_date="2024-01-31"
        status,sell_signal_date=check_sell(start_date,en)
        print(status,sell_signal_date)
        if status==True:
          # print('Sell Signal: ',check_sell(start_date,end_date))
          sell_signal_date = list(sell_signal_date)[0]  # Convert set to list and get first element
          # print(sell_signal_date)
          update_sell_signal_date(sell_signal_date, st_name, fixed_st, int(id))


          # stk=dfff.loc[i]['stock_name']
          st=sell_signal_date
          # id=dfff.loc[i]['id']
          print('Sell_Signal_Price')
          print(stk,st,id)
          # stk=dfff.loc[i]['stock_name']
          # st=sell_signal_date
          # st='2020-03-12'
          # lt=dfff.loc[i]['lead_date']

          if isinstance(st, datetime.date):
            st_date = st  # Already a date, no conversion needed
          else:
              st_date = datetime.datetime.strptime(st, "%Y-%m-%d").date()
          en_date = st_date + datetime.timedelta(days=10)
          st_date=st_date - datetime.timedelta(days=10)

          # ✅ Convert string to datetime and add one day
          # st_date = datetime.datetime.strptime(st, "%Y-%m-%d")
          # en_date = st_date + datetime.timedelta(days=1)

          # ✅ Convert back to string format
          en = en_date.strftime("%Y-%m-%d")
          stt=st_date.strftime("%Y-%m-%d")
          stp=st.strftime("%Y-%m-%d")

          # ✅ Function Call with Corrected Dates
          df = load_data_from_api(stk, stt, en)

          if df is not None and not df.empty:
            if not df[df['Date'] == stp].empty:
              ld_p=df[df['Date']==stp]['Close'].values[0]
              print(ld_p)
              update_sell_price(float(ld_p),stk,fixed_st,int(id))
            else:
              if isinstance(st, datetime.date):
                st_date = st  # Already a date, no conversion needed
              else:
                  st_date = datetime.datetime.strptime(st, "%Y-%m-%d").date()   ### ager friday ka data nhi ho ga to monday ki price append ho gi
              st_date=st_date + datetime.timedelta(days=3)
              res=is_friday(stp)
              if res == "Yes":
                stp=st_date.strftime("%Y-%m-%d")
                if not df[df['Date'] == stp].empty:
                  ld_p=df[df['Date']==stp]['Close'].values[0]
                  print(ld_p)
                  update_sell_price(float(ld_p),stk,fixed_st,int(id))
              else:
                df['Date'] = pd.to_datetime(df['Date'])
                stp = pd.to_datetime(stp)  # Convert to datetime

                # Filter for same or next available date
                filtered_dates = df[df['Date'] >= stp]

                # Update stp with the first available date after or equal to 'stp'
                if not filtered_dates.empty:
                    stp = filtered_dates['Date'].iloc[0]  # First available date

                stp = stp.strftime("%Y-%m-%d")
                if not df[df['Date'] == stp].empty:
                  ld_p=df[df['Date']==stp]['Close'].values[0]
                  print(ld_p)
                  update_sell_price(float(ld_p),stk,fixed_st,int(id))
                  
                  
    lead_dag() >> valid_signal_dag() >> price_dag()



leads_phases_script_dag = all_tasks_generate_taskflow_api()
