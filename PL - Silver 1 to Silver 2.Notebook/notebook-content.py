# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d2adfa13-c41e-476a-bd13-e44598122dbf",
# META       "default_lakehouse_name": "Silver_PL_1",
# META       "default_lakehouse_workspace_id": "c79766a3-4f30-43d3-942c-d1fa4e84b64d",
# META       "known_lakehouses": [
# META         {
# META           "id": "d2adfa13-c41e-476a-bd13-e44598122dbf"
# META         },
# META         {
# META           "id": "b4551982-caff-4d16-b567-3c06ac8fa2a6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## **Step 1. Verify Team Names Against Expected Variations (Team Mapping)**

# CELL ********************

# Define the team mapping dictionary
team_mapping = {
    "Arsenal": ["Arsenal"],
    "Aston Villa": ["Aston Villa"],
    "Barnsley": ["Barnsley"],
    "Birmingham City": ["Birmingham", "Birmingham City"],
    "Blackburn Rovers": ["Blackburn", "Blackburn Rovers" ],
    "Blackpool": ["Blackpool"],
    "Bolton": ["Bolton"],
    "Bournemouth": ["Bournemouth"],
    "Bradford": ["Bradford"],
    "Brentford": ["Brentford"],
    "Brighton and Hove Albion": ["Brighton", "Brighton and Hove Albion"],
    "Burnley": ["Burnley"],
    "Cardiff": ["Cardiff"],
    "Charlton": ["Charlton"],
    "Chelsea": ["Chelsea"],
    "Coventry": ["Coventry"],
    "Crystal Palace": ["Crystal Palace"],
    "Derby County": ["Derby", "Derby County"],
    "Everton": ["Everton"],
    "Fulham": ["Fulham"],
    "Huddersfield": ["Huddersfield"],
    "Hull": ["Hull"],
    "Ipswich": ["Ipswich"],
    "Leeds United": ["Leeds", "Leeds United"],
    "Leicester City": ["Leicester", "Leicester City"],
    "Liverpool": ["Liverpool"],
    "Luton Town": ["Luton"],
    "Manchester City": ["Man City", "Manchester City"],
    "Manchester United": ["Man United", "Man Utd", "Manchester Utd"],
    "Middlesbrough": ["Middlesbrough"],
    "Newcastle United": ["Newcastle", "Newcastle United"],
    "Norwich City": ["Norwich", "Norwich City"],
    "Nottingham Forest": ["Nott'm Forest", "Nottingham Forest"],
    "Oldham": ["Oldham"],
    "Portsmouth": ["Portsmouth"],
    "Queens Park Rangers": ["QPR", "Queens Park Rangers"],
    "Reading": ["Reading"],
    "Sheffield United": ["Sheffield United"],
    "Sheffield Wednesday": ["Sheffield Weds", "Sheffield Wednesday"],
    "Southampton": ["Southampton"],
    "Stoke City": ["Stoke", "Stoke City"],
    "Sunderland": ["Sunderland"],
    "Swansea City": ["Swansea", "Swansea City"],
    "Swindon": ["Swindon"],
    "Tottenham Hotspur": ["Tottenham", "Spurs", "Tottenham Hotspur"],
    "Watford": ["Watford"],
    "West Bromwich Albion": ["West Brom", "West Bromwich Albion"],
    "West Ham United": ["West Ham", "West Ham United"],
    "Wigan": ["Wigan"],
    "Wimbledon": ["Wimbledon"],
    "Wolverhampton Wanderers": ["Wolves", "Wolverhampton Wanderers"]
}

# Convert team mapping to a list of tuples (standard_name, variation)
team_mapping_data = [(standard, variation) for standard, variations in team_mapping.items() for variation in variations]
team_mapping_df = spark.createDataFrame(team_mapping_data, ["Standard_Team", "Variation"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the Silver 1 tables
df_schedule = spark.read.format("delta").table("Raw_Current_Schedule")
df_historical = spark.read.format("delta").table("Historical_Seasons")
df_current = spark.read.format("delta").table("Current_Season")
df_penalties = spark.read.format("delta").table("Point_Penalties")

# Extract distinct Home_Team and Away_Team
distinct_home_teams_schedule = df_schedule.select("Home_Team").distinct()
distinct_away_teams_schedule = df_schedule.select("Away_Team").distinct()

distinct_home_teams_historical = df_historical.select("Home_Team").distinct()
distinct_away_teams_historical = df_historical.select("Away_Team").distinct()

distinct_home_teams_current = df_current.select("Home_Team").distinct()
distinct_away_teams_current = df_current.select("Away_Team").distinct()

disitinct_penalised_teams = df_penalties.select("Team").distinct()

# Union all Home_Team and Away_Team into a single DataFrame
silver_1_teams = (distinct_home_teams_schedule.union(distinct_away_teams_schedule)
                                          .union(distinct_home_teams_historical)
                                          .union(distinct_away_teams_historical)
                                          .union(distinct_home_teams_current)
                                          .union(distinct_away_teams_current)
                                          .union(disitinct_penalised_teams)
                                          .distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Identify teams in Silver 1 Data Sources that are not found in team_mapping.
unexpected_teams = silver_1_teams.alias("dt").join(
    team_mapping_df.alias("tm"),
    F.col("dt.Home_Team") == F.col("tm.Variation"),
    "left_anti"
)

# Show teams not expected in data source (if any)
display(unexpected_teams)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if unexpected_teams.isEmpty():
    # Code to proceed
    print("Teams are as expected. Proceeding...")
else:
    raise ValueError("Unexpected teams appear in Silver 1. Please check your data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Step 2. Maintain Team Lookup and Ensure 1:1 Relationship with Standardized Names**

# CELL ********************

# Team Lookup
teams_data = [
    ["Arsenal", "https://resources.premierleague.com/premierleague/badges/t3.svg", "ARS", "#ff0000"],
    ["Aston Villa", "https://resources.premierleague.com/premierleague/badges/t7.svg", "AVL", "#490024"],
    ["Barnsley", "https://upload.wikimedia.org/wikipedia/en/thumb/c/c9/Barnsley_FC.svg/200px-Barnsley_FC.svg.png", "BAR", "#fd0000"],
    ["Birmingham City", "https://resources.premierleague.com/premierleague/badges/t41.svg", "BIR", "#0101de"],
    ["Blackburn Rovers", "https://resources.premierleague.com/premierleague/badges/t5.svg", "BBR", "#459df6"],
    ["Blackpool", "https://upload.wikimedia.org/wikipedia/en/thumb/d/df/Blackpool_FC_logo.svg/180px-Blackpool_FC_logo.svg.png", "BLA", "#FD743D"],
    ["Bolton", "https://upload.wikimedia.org/wikipedia/en/thumb/8/82/Bolton_Wanderers_FC_logo.svg/180px-Bolton_Wanderers_FC_logo.svg.png", "BOL", "#3240A0"],
    ["Bournemouth", "https://resources.premierleague.com/premierleague/badges/t91.svg", "BOU", "#C91318"],
    ["Bradford", "https://upload.wikimedia.org/wikipedia/en/thumb/3/32/Bradford_City_AFC.png/125px-Bradford_City_AFC.png", "BRA", "#6D1C29"],
    ["Brentford", "https://resources.premierleague.com/premierleague/badges/t94.svg", "BRE", "#fd0000"],
    ["Brighton and Hove Albion", "https://resources.premierleague.com/premierleague/badges/t36.svg", "BHA", "#0000fd"],
    ["Burnley", "https://resources.premierleague.com/premierleague/badges/t90.svg", "BRN", "#70193d"],
    ["Cardiff", "https://premierleague-static-files.s3.amazonaws.com/premierleague/badges/t97.svg", "CAR", "#0000e7"],
    ["Charlton", "https://upload.wikimedia.org/wikipedia/commons/thumb/6/6a/CharltonBadge_30Jan2020.png/800px-CharltonBadge_30Jan2020.png", "CHA", "#db0202"],
    ["Chelsea", "https://resources.premierleague.com/premierleague/badges/t8.svg", "CHE", "#0a4595"],
    ["Coventry", "https://upload.wikimedia.org/wikipedia/en/thumb/7/7b/Coventry_City_FC_crest.svg/1280px-Coventry_City_FC_crest.svg.png", "COV", "#80f2ef"],
    ["Crystal Palace", "https://resources.premierleague.com/premierleague/badges/t31.svg", "CRY", "#0055a5"],
    ["Derby County", "https://resources.premierleague.com/premierleague/badges/t24.svg", "DER", "#020202"],
    ["Everton", "https://resources.premierleague.com/premierleague/badges/t11.svg", "EVE", "#003399"],
    ["Fulham", "https://resources.premierleague.com/premierleague/badges/t54.svg", "FUL", "#090808"],
    ["Huddersfield", "https://premierleague-static-files.s3.amazonaws.com/premierleague/badges/t38.svg", "HUD", "#3899d7"],
    ["Hull", "https://premierleague-static-files.s3.amazonaws.com/premierleague/badges/t88.svg", "HUL", "#F7A212"],
    ["Ipswich", "https://upload.wikimedia.org/wikipedia/en/thumb/4/43/Ipswich_Town.svg/170px-Ipswich_Town.svg.png", "IPS", "#0000fc"],
    ["Leeds United", "https://resources.premierleague.com/premierleague/badges/t2.svg", "LEE", "#152F7B"],
    ["Leicester City", "https://resources.premierleague.com/premierleague/badges/t13.svg", "LEI", "#273e8a"],
    ["Liverpool", "https://resources.premierleague.com/premierleague/badges/t14.svg", "LIV", "#d3171e"],
    ["Luton Town", "https://upload.wikimedia.org/wikipedia/en/thumb/9/9d/Luton_Town_logo.svg/1920px-Luton_Town_logo.svg.png", "LTC", "#F78F1E"],
    ["Manchester City", "https://resources.premierleague.com/premierleague/badges/t43.svg", "MCI", "#87CEEB"],
    ["Manchester United", "https://resources.premierleague.com/premierleague/badges/t1.svg", "MUN", "#d20222"],
    ["Middlesbrough", "https://resources.premierleague.com/premierleague/badges/t25.svg", "MID", "#d60303"],
    ["Newcastle United", "https://resources.premierleague.com/premierleague/badges/t4.svg", "NEW", "#22b1ff"],
    ["Norwich City", "https://resources.premierleague.com/premierleague/badges/t45.svg", "NOR", "#008842"],
    ["Nottingham Forest", "https://resources.premierleague.com/premierleague/badges/t17.svg", "NFO", "#DC0202"],
    ["Oldham", "https://upload.wikimedia.org/wikipedia/en/thumb/1/19/Oldham_Athletic_FC.svg/1920px-Oldham_Athletic_FC.svg.png", "OLD", "#0000fd"],
    ["Portsmouth", "https://upload.wikimedia.org/wikipedia/en/thumb/3/38/Portsmouth_FC_logo.svg/1920px-Portsmouth_FC_logo.svg.png", "POR", "#0303a0"],
    ["Queens Park Rangers", "https://upload.wikimedia.org/wikipedia/en/thumb/3/31/Queens_Park_Rangers_crest.svg/1200px-Queens_Park_Rangers_crest.svg.png", "QPR", "#0053a3"],
    ["Reading", "https://resources.premierleague.com/premierleague/badges/t108.svg", "REA", "#0000fd"],
    ["Sheffield United", "https://upload.wikimedia.org/wikipedia/en/thumb/9/9c/Sheffield_United_FC_logo.svg/270px-Sheffield_United_FC_logo.svg.png", "SHU", "#F12228"],
    ["Sheffield Wednesday", "https://upload.wikimedia.org/wikipedia/en/thumb/8/88/Sheffield_Wednesday_badge.svg/150px-Sheffield_Wednesday_badge.svg.png", "SHW", "#0000fc"],
    ["Southampton", "https://resources.premierleague.com/premierleague/badges/t20.svg", "SOT", "#d71920"],
    ["Stoke City", "https://resources.premierleague.com/premierleague/badges/t110.svg", "STO", "#d7172f"],
    ["Sunderland", "https://resources.premierleague.com/premierleague/badges/t56.svg", "SUN", "#db001b"],
    ["Swansea City", "https://upload.wikimedia.org/wikipedia/en/thumb/f/f9/Swansea_City_AFC_logo.svg/380px-Swansea_City_AFC_logo.svg.png", "SWA", "#000000"],
    ["Swindon", "https://upload.wikimedia.org/wikipedia/en/thumb/a/a3/Swindon_Town_FC.svg/180px-Swindon_Town_FC.svg.png", "SWI", "#fc0000"],
    ["Tottenham Hotspur", "https://resources.premierleague.com/premierleague/badges/t6.svg", "TOT", "#0f204b"],
    ["Watford", "https://upload.wikimedia.org/wikipedia/en/thumb/e/e2/Watford.svg/255px-Watford.svg.png", "WAT", "#E3001B"],
    ["West Bromwich Albion", "https://resources.premierleague.com/premierleague/badges/t35.svg", "WBA", "#002868"],
    ["West Ham United", "https://resources.premierleague.com/premierleague/badges/t21.svg", "WHU", "#540d1a"],
    ["Wigan", "https://upload.wikimedia.org/wikipedia/en/thumb/4/43/Wigan_Athletic.svg/220px-Wigan_Athletic.svg.png", "WIG", "#0242A4"],
    ["Wimbledon", "https://upload.wikimedia.org/wikipedia/en/thumb/b/b3/Wimbledon_FC_crest.svg/1280px-Wimbledon_FC_crest.svg.png", "WIM", "#0000fd"],
    ["Wolverhampton Wanderers", "https://resources.premierleague.com/premierleague/badges/t39.svg", "WOL", "#FC891C"]
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create DataFrame For Lookup
df_teams = spark.createDataFrame(teams_data, ["Team", "ImageURL", "Abbreviations", "TeamColours"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DataFrame For Standard Team Names
Standard_Teams = team_mapping_df[['Standard_Team']].distinct()


NotInLookup = df_teams.alias("look").join(
    Standard_Teams.alias("tm"),
    F.col("look.Team") == F.col("tm.Standard_Team"),
    "left_anti"
)

NotInStandardTeams = Standard_Teams.alias("tm").join(
    df_teams.alias("look"),
      F.col("tm.Standard_Team") == F.col("look.Team"),
    "left_anti"
)


display(NotInLookup)
display(NotInStandardTeams)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if NotInLookup.isEmpty() and NotInStandardTeams.isEmpty():
    # Code to proceed
    print("Lookup is 1:1 with Standard Team Names. Proceeding...")
else:
    raise ValueError("Lookup and Standard Team Names are not aligned. Please check your data.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check there are no duplicate abbreviations 

duplicates_df = df_teams.groupBy("Abbreviations").count().filter("count > 1")

if duplicates_df.isEmpty() :
    # Code to proceed
    print("No duplicate abbreviations!")
else:
    raise ValueError("One or more duplicate abbreviations. Please check your data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Step 3. Clean Silver 1 Tables on Team Names**

# CELL ********************

#Function To Clean Team Names

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Broadcasting the team mapping dictionary for efficiency
team_mapping_broadcast = spark.sparkContext.broadcast(team_mapping)

# Define the function to map team variations to standardized names and raise an error if not found
def map_team_name(team):
    mapping = team_mapping_broadcast.value
    for standard, variations in mapping.items():
        if team in variations:
            return standard
    # Raise an error if the team is not found
    raise ValueError(f"Team name '{team}' not found in team mapping")

# Register the function as a UDF (user-defined function) in Spark
map_team_udf = F.udf(map_team_name, StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform Team Columns
df_cleaned_schedule = df_schedule.withColumn("Home_Team", map_team_udf(F.col("Home_Team"))) \
                         .withColumn("Away_Team", map_team_udf(F.col("Away_Team")))

df_cleaned_historical = df_historical.withColumn("Home_Team", map_team_udf(F.col("Home_Team"))) \
                         .withColumn("Away_Team", map_team_udf(F.col("Away_Team")))

df_cleaned_current = df_current.withColumn("Home_Team", map_team_udf(F.col("Home_Team"))) \
                         .withColumn("Away_Team", map_team_udf(F.col("Away_Team")))

df_cleaned_penalties = df_penalties.withColumn("Team", map_team_udf(F.col("Team")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Step 4. Load Clean Tables To Silver 2**

# CELL ********************

# Define the path to the lakehouse
lakehouse_path_schedule = "abfss://c79766a3-4f30-43d3-942c-d1fa4e84b64d@onelake.dfs.fabric.microsoft.com/b4551982-caff-4d16-b567-3c06ac8fa2a6/Tables/cleanedschedule"
lakehouse_path_historical = "abfss://c79766a3-4f30-43d3-942c-d1fa4e84b64d@onelake.dfs.fabric.microsoft.com/b4551982-caff-4d16-b567-3c06ac8fa2a6/Tables/cleanedhistorical"
lakehouse_path_current = "abfss://c79766a3-4f30-43d3-942c-d1fa4e84b64d@onelake.dfs.fabric.microsoft.com/b4551982-caff-4d16-b567-3c06ac8fa2a6/Tables/cleanedcurrent"
lakehouse_path_lookup = "abfss://c79766a3-4f30-43d3-942c-d1fa4e84b64d@onelake.dfs.fabric.microsoft.com/b4551982-caff-4d16-b567-3c06ac8fa2a6/Tables/teamlookup"
lakehouse_path_penalties = "abfss://c79766a3-4f30-43d3-942c-d1fa4e84b64d@onelake.dfs.fabric.microsoft.com/b4551982-caff-4d16-b567-3c06ac8fa2a6/Tables/pointpenalties"

# Write the cleaned DataFrame to the lakehouse in Delta format
df_cleaned_schedule.write.format("delta").mode("overwrite").save(lakehouse_path_schedule)
df_cleaned_historical.write.format("delta").mode("overwrite").save(lakehouse_path_historical)
df_cleaned_current.write.format("delta").mode("overwrite").save(lakehouse_path_current)
df_teams.write.format("delta").mode("overwrite").save(lakehouse_path_lookup)
df_cleaned_penalties.write.format("delta").mode("overwrite").save(lakehouse_path_penalties)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Generate Calendar**

# CELL ********************

from pyspark.sql.functions import col, min, max

# Create a summary table with Season, First Date, and Last Date
season_summary = df_cleaned_historical.groupBy("Season").agg(
    min("Match_Date").alias("First_Date_In_Season"),
    max("Match_Date").alias("Last_Date_In_Season")
)


# Create a summary table with Season, First Date, and Last Date
schedule_season_summary = df_cleaned_schedule.groupBy("Season").agg(
    min("Match_Date").alias("First_Date_In_Season"),
    max("Match_Date").alias("Last_Date_In_Season")
)

# Append the two tables
combined_season_summary = season_summary.union(schedule_season_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_add
from pyspark.sql import functions as F
from datetime import datetime

# Define start and end dates for the calendar
start_date =  df_cleaned_historical.agg(F.min("Match_Date")).collect()[0][0]
end_date = df_cleaned_schedule.agg(F.max("Match_Date")).collect()[0][0]
latest_match_date = datetime.today().date() 


# Create a DataFrame with a sequence of dates
date_range = spark.range(0, (spark.sql(f"SELECT datediff('{end_date}', '{start_date}')").collect()[0][0] + 1)) \
    .select(expr(f"date_add('{start_date}', cast(id as int)) as date"))



# Create a calendar table with additional columns
calendar_table = date_range.select(
    col("date"),
    expr("year(date) as year"),
 ((expr("month(date)") + 4 ) % 12).alias("month"),
    expr("day(date) as day"),
    expr("date_format(date, 'EEEE') as weekday"),  # Full name of the day
    expr("date_format(date, 'MMMM') as month_name"),  # Short month name
)

# Assuming combined_season_summary is your DataFrame with Season, First Date In Season, Last Date In Season
# Join the calendar table with the combined season summary
calendar_with_season = calendar_table.join(
    combined_season_summary,
    (col("date") >= col("First_Date_In_Season")) & (col("date") <= col("Last_Date_In_Season")),
    "left"
).select(
    col("date"),
    col("year"),
    col("month"),
    col("day"),
    col("weekday"),
    col("month_name"),
        # Replace null values in Season with "None"
    F.when(col("Season").isNull(), "None").otherwise(col("Season")).alias("Season")
    ).withColumn(
    "Has_A_Result_Date",
    F.when(col("date") <= F.lit(latest_match_date), "Has Result").otherwise("No Result")
)

   

lakehouse_path_calendar = "abfss://c79766a3-4f30-43d3-942c-d1fa4e84b64d@onelake.dfs.fabric.microsoft.com/b4551982-caff-4d16-b567-3c06ac8fa2a6/Tables/calendar"

# Display the updated calendar table with the season
calendar_with_season.write.format("delta").mode("overwrite").save(lakehouse_path_calendar)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
