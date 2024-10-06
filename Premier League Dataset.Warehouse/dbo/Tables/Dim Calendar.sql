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


GO
ALTER TABLE [dbo].[Dim Calendar] ADD CONSTRAINT UQ_143781dd_544e_4c1f_8963_2e1011c7d2ee unique NONCLUSTERED ([Date]);
