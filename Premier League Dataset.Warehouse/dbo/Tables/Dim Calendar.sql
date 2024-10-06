CREATE TABLE [dbo].[Dim Calendar] (

	[Date] date NULL, 
	[Year] int NULL, 
	[Month] int NULL, 
	[Day] int NULL, 
	[Weekday] varchar(8000) NULL, 
	[Month Name] varchar(8000) NULL, 
	[Season] varchar(8000) NULL, 
	[Has_A_Result_Date] varchar(8000) NULL
);

