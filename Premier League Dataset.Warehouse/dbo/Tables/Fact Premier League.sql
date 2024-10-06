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


GO
ALTER TABLE [dbo].[Fact Premier League] ADD CONSTRAINT FK_064c2429_e5be_425f_b1f1_43c28996908e FOREIGN KEY ([Home_Team]) REFERENCES dbo.Dim Home Team([Home Team]);
GO
ALTER TABLE [dbo].[Fact Premier League] ADD CONSTRAINT FK_787a89d4_9c88_4113_ba36_f6ba3db96621 FOREIGN KEY ([Opposition]) REFERENCES dbo.Dim Opposition([Opponent]);
GO
ALTER TABLE [dbo].[Fact Premier League] ADD CONSTRAINT FK_8d826820_eb4d_440e_aacc_ce374207d1c5 FOREIGN KEY ([Away_Team]) REFERENCES dbo.Dim Away Team([Away Team]);
GO
ALTER TABLE [dbo].[Fact Premier League] ADD CONSTRAINT FK_ba8a4ecf_fe74_40dc_b04f_6d7d542c1db2 FOREIGN KEY ([Match_Date]) REFERENCES dbo.Dim Calendar([Date]);
