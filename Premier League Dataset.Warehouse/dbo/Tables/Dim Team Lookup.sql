CREATE TABLE [dbo].[Dim Team Lookup] (

	[Team] varchar(8000) NULL, 
	[ImageURL] varchar(8000) NULL, 
	[Abbreviations] varchar(8000) NULL, 
	[TeamColours] varchar(8000) NULL
);


GO
ALTER TABLE [dbo].[Dim Team Lookup] ADD CONSTRAINT UQ_9ba0db24_4102_4e18_a042_6c32019ecc1d unique NONCLUSTERED ([Team]);
GO
ALTER TABLE [dbo].[Dim Team Lookup] ADD CONSTRAINT FK_5e09f05e_a4ce_43c4_ab1e_4ec7c9f1736d FOREIGN KEY ([Team]) REFERENCES dbo.Tooltip Team Lookup([Team]);
