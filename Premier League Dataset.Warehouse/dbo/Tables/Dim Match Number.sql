CREATE TABLE [dbo].[Dim Match Number] (

	[Match Number] bigint NULL
);


GO
ALTER TABLE [dbo].[Dim Match Number] ADD CONSTRAINT UQ_30a3d84c_152a_48fe_84bd_93fe44882e5f unique NONCLUSTERED ([Match Number]);
