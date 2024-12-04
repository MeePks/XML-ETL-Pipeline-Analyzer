/****** Script for SelectTopNRows command from SSMS  ******/
Declare @DBname varchar(200)='GiantEagleDPGroupTracker'
Declare @Auditname Varchar(max)='GiantEagle';
declare @Actiantablename varchar(200)=@dbName+'.dbo.TrackerLoadLog_Actain'
declare @SSISTablename varchar(200)=@dbName+'.dbo.TrackerLoadLog'
Declare @sql varchar(max)=''

Set @sql='With TrackerLoadLogs as(
  select A.mapname,
  Reverse(Substring(REVERSE(a.Mapname),1,CHARindex(''\'',REVERSE(a.Mapname))-1)) ActianMap,
  Reverse(Substring(REVERSE(s.Mapname),1,CHARindex(''\'',REVERSE(s.Mapname))-1)) PackageName,
  isnull(a.FileName,s.FileName) As Filename,
  ''dbo.''+a.TableName ActianTableName,
  s.TableName SSISTableName,
  a.recordsRead ActianReadCount,
  s.RecordsRead SsisReadCount,
  a.RecordsWritten ActianWrittenRecod,
  s.RecordsWritten SSISWrittenCount
  FROM '+@Actiantablename+' a
  inner join  '+@SSISTablename+' s
  on a.filename=s.FileName
  and Replace(Reverse(Substring(REVERSE(a.Mapname),1,CHARindex(''\'',REVERSE(a.Mapname))-1)),''.map.xml'','''')=Replace(Reverse(Substring(REVERSE(s.Mapname),1,CHARindex(''\'',REVERSE(s.Mapname))-1)),''.dtsx'','''')
   ),
   Alldetails as
   (
    select AuditId,Packid,AuditName,tll.*,(ActianReadCount-SsisReadCount) ReadCntDiff,(ActianWrittenRecod-SSISWrittenCount) WriteCntDiff--select *
	from [UAT_RCA].[dbo].[UAT_ClientPackages] ucp
	inner join TrackerLoadLogs tll on ucp.ActainLoadTableName=tll.ActianTableName
	and ucp.SSISLoadTableName=tll.SSISTableName
	where  ucp.AuditName='''+@Auditname+ ''' 
   )
  Update [UAT_RCA].[dbo].[ProcessAppleComparison] 
  set [HasTrackerLoadReadCntDifference]= Case when ReadCntDiff=0 then 0 else 1 End,
	[HasTrackerLoadWrtCntDifference]= Case when WriteCntDiff=0 then 0 else 1 End
  From Alldetails  ad
  where ProcessAppleComparison.auditid=ad.auditid
  and ProcessAppleComparison.PackID=ad.PackId'

  EXEC (@sql)







