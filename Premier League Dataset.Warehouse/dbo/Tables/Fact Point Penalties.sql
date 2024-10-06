CREATE TABLE [dbo].[Fact Point Penalties] (

	[Date] date NULL, 
	[Team] varchar(8000) NULL, 
	[Points] bigint NULL, 
	[Match_Number] bigint NULL
);


GO
ALTER TABLE [dbo].[Fact Point Penalties] ADD CONSTRAINT FK_846d0e11_79e4_49a9_a134_5144f91d039b FOREIGN KEY ([Team]) REFERENCES dbo.Dim Team Lookup([Team]);
GO
ALTER TABLE [dbo].[Fact Point Penalties] ADD CONSTRAINT FK_ac2a19f2_44e2_479c_a138_71b5c4498435 FOREIGN KEY ([Date]) REFERENCES dbo.Dim Calendar([Date]);
GO
ALTER TABLE [dbo].[Fact Point Penalties] ADD CONSTRAINT FK_e47d6eb2_f22d_43c1_8b5e_980d23d48be0 FOREIGN KEY ([Match_Number]) REFERENCES dbo.Dim Match Number([Match Number]);
