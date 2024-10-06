CREATE TABLE [dbo].[Fact Premier League] (

	[Season] varchar(8000) NULL, 
	[Team] varchar(8000) NULL, 
	[Match_Date] date NULL, 
	[Home_Team] varchar(8000) NULL, 
	[Away_Team] varchar(8000) NULL, 
	[TeamGoalsScored] bigint NULL, 
	[TeamGoalsConceded] bigint NULL, 
	[Full_Time_Result] varchar(8000) NULL, 
	[TeamShots] bigint NULL, 
	[TeamShotsConceded] bigint NULL, 
	[TeamShotsOnTarget] bigint NULL, 
	[TeamShotsOnTargetConceded] bigint NULL, 
	[TeamCorners] bigint NULL, 
	[TeamCornersConceded] bigint NULL, 
	[TeamFouls] bigint NULL, 
	[OpponentFouls] bigint NULL, 
	[TeamYellowCards] bigint NULL, 
	[OpponentYellowCards] bigint NULL, 
	[TeamRedCards] bigint NULL, 
	[OpponentRedCards] bigint NULL, 
	[Score] varchar(8000) NULL, 
	[Result] varchar(8000) NULL, 
	[Match Number] bigint NULL, 
	[TeamIsHome] varchar(8000) NULL, 
	[Opposition] varchar(8000) NULL, 
	[TotalGoalsInMatch] bigint NULL
);

