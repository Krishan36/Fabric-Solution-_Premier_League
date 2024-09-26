CREATE TABLE [dbo].[Dim Home Team] (

	[Home Team] varchar(8000) NULL, 
	[ImageURL] varchar(8000) NULL, 
	[Abbreviations] varchar(8000) NULL, 
	[TeamColours] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[Dim Home Team] ADD CONSTRAINT UQ_e8c4aa45_d62d_4aa8_ac2b_40ca2c1e6b40 unique NONCLUSTERED ([Home Team]);