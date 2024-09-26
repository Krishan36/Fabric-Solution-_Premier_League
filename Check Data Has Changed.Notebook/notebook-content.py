# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2adfa13-c41e-476a-bd13-e44598122dbf",
# META       "default_lakehouse_name": "Silver_PL",
# META       "default_lakehouse_workspace_id": "c79766a3-4f30-43d3-942c-d1fa4e84b64d"
# META     }
# META   }
# META }

# CELL ********************

# Load current season data from the Delta table
df_current = spark.read.format("delta").table("Current_Season")
current_count = df_current.count()

# Import necessary libraries
import pandas as pd
import json

# Define the URL of the CSV file
csv_url = "https://www.football-data.co.uk/mmz4281/2425/E0.csv"

try:
    # Attempt to load the CSV
    pdf = pd.read_csv(csv_url)
    
    # Convert the pandas DataFrame to a Spark DataFrame
    df_live = spark.createDataFrame(pdf)
    


    # Count the number of records in the live data
    live_count = df_live.count()

except Exception as e:
    # If an error occurs, set live_count to 0
    live_count = 0



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

    # Keep only 'Home Team' and 'Away Team' columns
    df_f = df_html[['Home Team', 'Away Team']]

    # Display the final DataFrame (either valid data or blank)
    df_filtered = spark.createDataFrame(df_f)
    df_schedule = spark.read.format("delta").table("Raw_Current_Schedule")[['Home_Team', 'Away_Team']]
    
    # Rename columns in df_filtered to match df_schedule
    df_filtered_renamed = df_filtered \
    .withColumnRenamed('Home Team', 'Home_Team') \
    .withColumnRenamed('Away Team', 'Away_Team')
    
    df_schedule_sorted = df_schedule.orderBy(['Home_Team', 'Away_Team'])
    df_filtered_sorted = df_filtered_renamed.orderBy(['Home_Team', 'Away_Team'])
    
    # Check if the two sorted DataFrames are equivalent by collecting the data as lists for comparison
    are_equal = df_filtered_sorted.collect() == df_schedule_sorted.collect()
    
    # Store the result in a variable
    SameHomeAndAway = "Yes" if are_equal else "No"

    df_f1 = df_html[['Date','Home Team', 'Away Team']]
    df_filtered1 = spark.createDataFrame(df_f1)

    # Rename columns in df_filtered1 to match df_schedule1
    df_filtered_renamed1 = df_filtered1 \
    .withColumnRenamed('Date', 'Match_Date') \
    .withColumnRenamed('Home Team', 'Home_Team') \
    .withColumnRenamed('Away Team', 'Away_Team') \
    .withColumn('Match_Date', to_date(to_timestamp(col('Match_Date'), 'dd/MM/yyyy HH:mm')))
    
    df_schedule1 = spark.read.format("delta").table("Raw_Current_Schedule")[['Match_Date','Home_Team', 'Away_Team']]

    df_schedule_sorted1 = df_schedule1.orderBy(['Match_Date', 'Home_Team', 'Away_Team'])
    df_filtered_sorted1 = df_filtered_renamed1.orderBy(['Match_Date', 'Home_Team', 'Away_Team'])

    # Check if the two sorted DataFrames are equivalent by collecting the data as lists for comparison
    are_equal1 = df_filtered_sorted1.collect() == df_schedule_sorted1.collect()

    SameSchedule = "Yes" if are_equal1 else "No"

except requests.exceptions.RequestException as e:
    SameHomeAndAway = "No Data"
    SameSchedule = "No Data"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the output JSON object
output_json = {
    "current_count": current_count,
    "live_count": live_count,
    "same_home_away": SameHomeAndAway,
    "same_schedule": SameSchedule
}

# Convert the output to a JSON string and exit the notebook with the result
mssparkutils.notebook.exit(json.dumps(output_json))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
