CREATE TABLE [dbo].[Tooltip Team Lookup] (

	[Team] varchar(8000) NULL, 
	[ImageURL] varchar(8000) NULL, 
	[Abbreviations] varchar(8000) NULL, 
	[TeamColours] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[Tooltip Team Lookup] ADD CONSTRAINT UQ_e457ed08_41d7_4af0_aefe_6d4afd2f9d6d unique NONCLUSTERED ([Team]);