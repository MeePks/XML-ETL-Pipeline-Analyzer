--Declare these 3 necessary variables 
Declare @mapnametfxml varchar(124)='Po1Q_FTP.tf.xml'
Declare @auditname varchar(20)='walmartmx'
Declare @phase char(2)=5


--these will be setup by the query itself
Declare @packid varchar(10)=''
Declare @actianTableName varchar(max)=''
Declare @SSISTableName varchar(max)=''
Declare @file varchar(max)=''
Declare @trackerdb varchar(max)=''

--Get information of the tracker database
 select @trackerdb=AuditProcGroupTrackerDB
  FROM [UAT_RCA].[dbo].[UAT_ClientInformation] a
	where auditname like '%'+@auditname+'%'
	and UATphases=@phase

--Get information of the Load Details
select @packid=packid,@actianTableName=ActainLoadTableName,@SSISTableName=SSISLoadTableName,@file=AuditDataFileLocation
  FROM [UAT_RCA].[dbo].[UAT_ClientPackages] a
	where auditname like '%'+@auditname+'%'
	and UATPhases in (@phase)
	and [ActainMaps] like '%'+@mapnametfxml+'%'

--Display the Load information with package
select PackId,AuditName,SSISPackages,UATPhases,ConnectorType,PackageStatus,ActainLoadTableName,SSISLoadtablename,ActainLoadStatus,SSISLoadStatus,controlmjobname
--select *
	FROM [UAT_RCA].[dbo].[UAT_ClientPackages] a
		where auditname like '%'+@auditname+'%'
		and UATPhases in (@phase)
		and [ActainMaps] like '%'+@mapnametfxml+'%'

--display variable infromations
DECLARE @sql NVARCHAR(MAX);
SET @sql = N'SELECT * FROM ' 
SET @sql+= QUOTENAME(@trackerdb)+'.dbo.VariablesInfo '
SET @sql+='where mapname ='''+@mapnametfxml+''''
EXEC sp_executesql @sql;

--display and set the details of Apple to Apple Comparision
Declare @datadiff char(1)=''
Declare @tablediff char(1)=''
SELECT @datadiff=HasDataDiffernece,@tablediff=HasTemplateDifference
  FROM [UAT_RCA].[dbo].[ProcessAppleComparison]
	where packid in ( @packid)

SELECT *
  FROM [UAT_RCA].[dbo].[ProcessAppleComparison]
	where packid in ( @packid)

--Display necessary differences if found Any
If @tablediff=1
	Begin
	 select * 
		from [UAT_RCA].[dbo].[TableStructureDiffernece]
		where  packid in (@packid)
	End

If @datadiff=1
	Begin
		SELECT *
		FROM [UAT_RCA].[dbo].[DataStructureDifferences]
		where  packid in ( @packid)
	End

--Dispalying the details of trackerLoadLog Infromations
SET @sql = N'SELECT DataGroup,FileName,MapName,TableName,SFileName,RecordsRead,RecordsWritten,FileSize,SqlDataBase FROM ' 
SET @sql+= QUOTENAME(@trackerdb)+'.dbo.TrackerLoadLog_Actain '
SET @sql+='where mapname like ''%'+Replace(@mapnametfxml,'.tf.xml','')+'%'''
EXEC sp_executesql @sql;


SET @sql = N' SELECT DataGroup,FileName,MapName,TableName,SFileName,RecordsRead,RecordsWritten,FileSize,SqlDataBase FROM ' 
SET @sql+= QUOTENAME(@trackerdb)+'.dbo.TrackerLoadLog '
SET @sql+='where mapname like ''%'+Replace(@mapnametfxml,'.tf.xml','')+'%'''
EXEC sp_executesql @sql;

Select *
FROM [RcaDPTeam].[dbo].[_vw_SpecialEvents]
  where auditname=@auditname
  and mapname=@mapnametfxml

