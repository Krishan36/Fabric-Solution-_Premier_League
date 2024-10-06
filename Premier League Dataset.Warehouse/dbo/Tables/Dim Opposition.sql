CREATE TABLE [dbo].[Dim Opposition] (

	[Opponent] varchar(8000) NULL, 
	[ImageURL] varchar(8000) NULL, 
	[Abbreviations] varchar(8000) NULL, 
	[TeamColours] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[Dim Opposition] ADD CONSTRAINT UQ_beae44f2_49b3_46d7_bfd1_d521eedee469 unique NONCLUSTERED ([Opponent]);