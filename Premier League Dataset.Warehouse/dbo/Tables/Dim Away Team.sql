CREATE TABLE [dbo].[Dim Away Team] (
	[Away Team] varchar(8000) NULL, 
	[ImageURL] varchar(8000) NULL, 
	[Abbreviations] varchar(8000) NULL, 
	[TeamColours] varchar(8000) NULL
);



GO
ALTER TABLE [dbo].[Dim Away Team] ADD CONSTRAINT UQ_a2d64fb1_ee75_4ff2_a940_dd645b27f49e unique NONCLUSTERED ([Away Team]);
