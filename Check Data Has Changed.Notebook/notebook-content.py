# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "72ab087e-c6f4-40dd-8149-95bd9c8e5bb5",
# META       "default_lakehouse_name": "Bronze_PL",
# META       "default_lakehouse_workspace_id": "c79766a3-4f30-43d3-942c-d1fa4e84b64d",
# META       "known_lakehouses": [
# META         {
# META           "id": "72ab087e-c6f4-40dd-8149-95bd9c8e5bb5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Import necessary libraries
import pandas as pd
import json


# Load current season data from the Delta table
df_current = spark.read.format("delta").table("Current_Season")

# Define the URL of the CSV file
csv_url = "https://www.football-data.co.uk/mmz4281/2425/E0.csv"

try:
    # Attempt to load the CSV
    pdf = pd.read_csv(csv_url)
    
    # Convert the pandas DataFrame to a Spark DataFrame
    df_live = spark.createDataFrame(pdf)

    # Perform a left anti-join to find rows in df_current that are not in df_live
    missing_in_live = df_current.join(df_live, df_current.columns, how='left_anti')

    # Check if the result is empty
    refreshcurrent = "No" if missing_in_live.count() == 0 else "Yes"

except Exception as e:
    refreshcurrent = "No"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql.functions import to_date, col, to_timestamp

# URL to scrape
url_schedule = "https://fixturedownload.com/results/epl-2024"

try:
    # Send a GET request to fetch the HTML content
    response = requests.get(url_schedule)
    response.raise_for_status()  # Raise an error if the status code is not 200 (OK)

    # Parse the HTML with BeautifulSoup
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Define the RowSelector and Column Selectors
    rows = soup.select("TABLE > * > TR")

    # Prepare a list to store the data
    table_data = []

    # Iterate through each row
    for row in rows:
        # Extract each column based on the nth-child() selector
        col1 = row.select_one("TABLE > * > TR > :nth-child(1)").text if row.select_one("TABLE > * > TR > :nth-child(1)") else None
        col2 = row.select_one("TABLE > * > TR > :nth-child(2)").text if row.select_one("TABLE > * > TR > :nth-child(2)") else None
        col3 = row.select_one("TABLE > * > TR > :nth-child(3)").text if row.select_one("TABLE > * > TR > :nth-child(3)") else None
        col4 = row.select_one("TABLE > * > TR > :nth-child(4)").text if row.select_one("TABLE > * > TR > :nth-child(4)") else None  # Home Team
        col5 = row.select_one("TABLE > * > TR > :nth-child(5)").text if row.select_one("TABLE > * > TR > :nth-child(5)") else None
        col6 = row.select_one("TABLE > * > TR > :nth-child(6)").text if row.select_one("TABLE > * > TR > :nth-child(6)") else None  # Away Team
        
        # Append the row data to the table_data list
        table_data.append([col1, col2, col3, col4, col5, col6])

    # Promote the first row as header and remove it from the data
    headers = table_data[0]  # First row becomes the header
    data = table_data[1:]  # All subsequent rows are data

    # Create the DataFrame with promoted headers
    df_html = pd.DataFrame(data, columns=headers)

    # Convert to Spark DataFrame
    df_live_sch = spark.createDataFrame(df_html)

    # Rename Home Team and Away Team columns
    df_live_sch_renamed = df_live_sch \
    .withColumnRenamed('Home Team', 'Home_Team') \
    .withColumnRenamed('Away Team', 'Away_Team')
    
    # Load Data Stored In Bronze Layer
    df_schedule = spark.read.format("delta").table("Raw_Current_Schedule")

    # Compare Home_Team and Away_Team columns
    df_schedule_HA = df_schedule[['Home_Team', 'Away_Team']]
    df_live_HA = df_live_sch_renamed[['Home_Team','Away_Team']]

    DifferentTeams = df_schedule_HA.join(df_live_HA, df_schedule_HA.columns, how='left_anti')

    # Compare Date Home_Team and Away_Team columns
    df_schedule_DHA = df_schedule[['Date', 'Home_Team', 'Away_Team']]
    df_live_DHA = df_live_sch_renamed[['Date', 'Home_Team','Away_Team']]

    changed_schedule = df_schedule_DHA.join(df_live_DHA, df_schedule_DHA.columns, how='left_anti')  

    if DifferentTeams.count() == 0:
        if changed_schedule.count() == 0:
             refreshschedule = "No"
        else:
            refreshschedule = "Yes"
    else:
        refreshschedule = "No"

except requests.exceptions.RequestException as e:
    refreshschedule = "No"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the output JSON object
output_json = {
    "refresh_current": refreshcurrent,
    "refresh_schedule": refreshschedule
}

# Convert the output to a JSON string and exit the notebook with the result
mssparkutils.notebook.exit(json.dumps(output_json))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
