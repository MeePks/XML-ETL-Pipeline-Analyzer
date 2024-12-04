import os
import xml.etree.ElementTree as ET
import pyodbc
import shutil
import os.path

#Give the  AuditName, serverName and DatabaseName of the Audit where Tracking Maps Table is Available
audit_name="HomeDepot"
server_name="" #servername in which trackingmaps table is available
db_name=""   #databse in which tracking Maps table is .
TrackerLoadLogDB=""
actian_copy_path=r"" #copies XML file so that the orignal files doesn't change
if not os.path.exists(actian_copy_path):
    os.makedirs(actian_copy_path)

#connecting to the Tracking Maps Table 
try:
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={db_name};Trusted_Connection=yes;'
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    print("Sucessfully Connected to the Tracking Maps Database")
    
except Exception as e:
    print(f"Error connecting to SQL Server: {e}") 


# Check if the table exists
table_check_query = "IF OBJECT_ID('Tracking_Maps', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0"
cursor.execute(table_check_query)
table_exists = cursor.fetchone()[0]

# Display The Information
print(f"Checking Tracking_Maps table existence on {server_name}...")

if table_exists:
    #Connecting to the destination tables server for inserting Map Events
    try:
        print("Table exists........ Copying maps from prod to test location..... ")
        dst_connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=GAD1UCHCSSIS001;DATABASE=RcaDPTeam;Trusted_Connection=yes;'
        dst_connection = pyodbc.connect(dst_connection_string)
        dst_cursor = dst_connection.cursor()
        print("Sucessfully Connected to the RCADpTeam Database")
        
    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")

    #first Create MapEvents Table if not exists
    dst_cursor.execute('''
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MapEvents')

        CREATE TABLE dbo.MapEvents (
            AuditName NVARCHAR(50),
            MapId NVARCHAR(10),
            MapFolder NVARCHAR(100),
            MapName NVARCHAR(255),
            EventSource NVARCHAR(255),
            EventName NVARCHAR(255),
            ActionName NVARCHAR(255),
            ParameterName NVARCHAR(255),
            ParameterValue NVARCHAR(MAX),
            AutoID int identity(1,1)
        )
        ''')

    #tables for inserting source and target Informations....
    srctbl=("")
    tgttbl=("")

    # Execute a query to get the list of Maps in Traking Table
    
    query = f"select * from dbo.tracking_maps where ActiveMapBit=1"
    cursor.execute(query)
 
    # Fetch all the rows with Map Details with Maps Details
    rows = cursor.fetchall()
    for row in rows:
        TfXMLPath=row.MapFolder+"\\"+row.MapName
        MapXMLPath=TfXMLPath.replace('tf.xml','map.xml')

        #inserting Map Information In MapExceptionTable if Map Information is not available already
        dst_cursor.execute("select 1 from dbo.MapException where MapID=? and AuditName=?",row.MapID,audit_name)
        row_count= dst_cursor.fetchone()

        if row_count is None:
            dst_cursor.execute("INSERT INTO dbo.MapException (AuditName, MapId,MapFolder,MapName) VALUES (?, ?, ?, ?)",
                               audit_name, row.MapID, row.MapFolder,row.MapName)

        #copying tf.xml and map.xml to test path
        if os.path.isfile(TfXMLPath):
            shutil.copy2(TfXMLPath,actian_copy_path)  
            shutil.copy2(MapXMLPath,actian_copy_path)
            TestMapPath=actian_copy_path+"\\"+row.MapName.replace('tf.xml','map.xml')
            print(TestMapPath)
            tree = ET.parse(TestMapPath)
            root = tree.getroot()

            #Pulling tf.xml Informations 
            TestTfPath=actian_copy_path+"\\"+row.MapName
            treetf = ET.parse(TestTfPath)
            roottf = treetf.getroot()
            for tfdetails in roottf.findall(".//TransformationSources/TransformationSource"):
                connector_name =tfdetails.get('connectorname')
                options_details={}
                for option in tfdetails.findall('.//TransformationSourceOptions/Option'):
                    option_name=option.get('name')
                    option_value=option.get('value')
                    options_details[option_name]=option_value

            error_details={}
            for ErrorHandling in roottf.findall('.//TransformationOptions/Option'):
                    Error_name=ErrorHandling.get('name')
                    Error_value=ErrorHandling.get('value')
                    error_details[Error_name]=Error_value

            for tfdetails in roottf.findall(".//TransformationTargets/TransformationTarget"):
                output_mode =tfdetails.get('outputmode')

            codepage_value =options_details.get('codepage')
            recsep =options_details.get('recsep')
            fldsep =options_details.get('fldsep')
            fldsdelim =options_details.get('fldsdelim')
            fldedelim =options_details.get('fldedelim')
            header =options_details.get('header')
            soffset =options_details.get('soffset')
            layoutmismatch =options_details.get('layoutmismatch')
            TruncationHandling =error_details.get('truncationhandling')
            OverFlowHandling =error_details.get('overflowhandling')
            #print(audit_name, row.MapID, row.MapFolder,row.MapName,connector_name,codepage_value,recsep,fldsep,fldsdelim,fldedelim,header,soffset,TruncationHandling,OverFlowHandling,layoutmismatch,output_mode)
            dst_cursor.execute("INSERT INTO MapHeader (AuditName, MapId,MapFolder,MapName,ConnectorType,CodePage,RecordSeperator,FieldSeperator,FieldStartDelimeter,FieldEndDelimeter,Header,StartOffset,TruncationHandling,OverFlowHandling,LayoutMismatch,outputmode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               audit_name, row.MapID, row.MapFolder,row.MapName,connector_name,codepage_value,recsep,fldsep,fldsdelim,fldedelim,header,soffset,TruncationHandling,OverFlowHandling,layoutmismatch,output_mode)
            

            #pulling source filters if available
            #pulling source filters if available
            for filters in roottf.findall('.//DataAccess/FilterExpressions/'):
                filterexp=filters.get("language")
                if filterexp is not None:
                        exp=filters.text
                        dst_cursor.execute("INSERT INTO MapEvents (AuditName, MapId,MapFolder,MapName,EventSource,EventName,ActionName,ParameterName,ParameterValue) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               audit_name, row.MapID, row.MapFolder,row.MapName,"SourceFilter","Filter","Filter Source",filterexp,exp)

            #Pulling the source Information
            for map_source in root.findall(".//MapSources/MapSource"):
                map_schema = map_source.find("MapSchema")
                if map_schema is not None:
                    for record_layout in map_schema.findall(".//RecordLayout"):
                        for field in record_layout.findall(".//Field"):
                            field_name = field.get("name")
                            data_type = field.find(".//Datatype").get("dataalias")
                            data_length = field.find(".//Datatype").get("datalength")
            #print(field_name+' '+data_type+' '+data_length )

            #Pulling the source Event Information
            for source_event in root.findall(".//MapSource/RecordLayoutEvents/Event"):
                src_event_name=source_event.get("name")
                for src_action in source_event.findall(".//Action"):
                    src_action_name=src_action.get("name")
                    for src_parameters in src_action.findall(".//Parameter"):
                        src_paramter_name=src_parameters.get("name")
                        src_paramter_value=src_parameters.text
                        #print(audit_name, row.MapID, row.MapFolder,row.MapName,"Source",src_event_name,src_action_name,src_paramter_name,src_paramter_value)
                        dst_cursor.execute("INSERT INTO MapEvents (AuditName, MapId,MapFolder,MapName,EventSource,EventName,ActionName,ParameterName,ParameterValue) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               audit_name, row.MapID, row.MapFolder,row.MapName,"Source",src_event_name,src_action_name,src_paramter_name,src_paramter_value)
                        #print("Source Events  " + src_event_name  +"  "+src_action_name+"   "+src_paramter_name +"   "+src_paramter_value  )


            #Pulling the Target Information
            for map_target in root.findall(".//MapTargets/MapTarget"):
                map_schema = map_target.find("MapSchema")
                if map_schema is not None:
                    for record_layout in map_schema.findall(".//RecordLayout"):
                        for field in record_layout.findall(".//Field"):
                            field_name = field.get("name")
                            data_type = field.find(".//Datatype").get("dataalias")
                            data_length = field.find(".//Datatype").get("datalength")
                            #print(field_name+' '+data_type+' '+data_length )


            #Pulling the Target Event Information
            for target_event in root.findall(".//MapTarget/RecordLayoutEvents/Event"):
                target_event_name=target_event.get("name")
                for target_action in target_event.findall(".//Action"):
                    target_action_name=target_action.get("name")
                    for target_parameters in target_action.findall(".//Parameter"):
                        target_paramter_name=target_parameters.get("name")
                        target_paramter_value=target_parameters.text
                        dst_cursor.execute("INSERT INTO MapEvents (AuditName, MapId,MapFolder,MapName,EventSource,EventName,ActionName,ParameterName,ParameterValue) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               audit_name, row.MapID, row.MapFolder,row.MapName,"Target",target_event_name,target_action_name,target_paramter_name,target_paramter_value)
                        #print("Target Events  " + target_event_name  +"  "+target_action_name+"   "+target_paramter_name +"   "+target_paramter_value  )



            #pulling the value of MapEvents
            for map_event in root.findall(".//MapEvents/Event"):
                event_name = map_event.get("name")
                for action in map_event.findall(".//Action"):
                    action_name = action.get("name")
                    for parameter in action.findall(".//Parameter"):
                        paramter_name=parameter.get("name")             
                        parameter_value = parameter.text
                        dst_cursor.execute("INSERT INTO MapEvents (AuditName, MapId,MapFolder,MapName,EventSource,EventName,ActionName,ParameterName,ParameterValue) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               audit_name, row.MapID, row.MapFolder,row.MapName,"Map",event_name,action_name,paramter_name,parameter_value)
                        #print(event_name +' '+action_name +' '+paramter_name+' '+parameter_value)

        else:
            dst_cursor.execute("update [dbo].[MapException] set IsAvailable=0 where AuditName=? and MapId=?",audit_name, row.MapID)
    
    #pulling Information from TrackerLoadLogTable and dumping it into our server
    connecttracker=f"USE {TrackerLoadLogDB}"
    cursor.execute(connecttracker)
    cursor.execute("select ?,Mapname,MAx(FileName),Max([SQLServer]),Max([SQLDatabase]),Max(TableName),Max(loadstartdate) from trackerLoadLog where year(loadstartdate)>2021 Group by Mapname",audit_name)
    mapname_transfer=cursor.fetchall()
    dst_cursor.executemany("INSERT INTO MapsInTrackerLoadLog (AuditName,MapName,FileName,SQLServer,DatabaseName,TableName,LastLoadDate) Values(?,?,?,?,?,?,?)",mapname_transfer)

    #removing old data from MapEventsTables and Maps in TrackerLoadLogTable
    dst_cursor.execute('''
        With Cte as(
        select *,Row_number()over(partition by auditname,mapid,Eventsource,EventName,ActionName,parametername,parametervalue order by autoid desc) rn
        from [RcaDPTeam].[dbo].[MapEvents]
        where AuditName=?
        )
        delete from cte where rn<>1
                ''',audit_name
        )

    dst_cursor.execute('''
            With cte as(
            select *,Row_number()over(Partition BY Auditname,Mapname order by autoid desc ) as rn from dbo.MapsInTrackerLoadLog
            where AuditName=?
            )
            delete from cte where rn<>1
            ''',audit_name
                   )

    dst_cursor.execute('''
            With cte as(
            select *,Row_number()over(Partition BY Auditname,Mapid,Mapname order by autoid desc ) as rn from dbo.MapHeader
            where AuditName=?
            )
            delete from cte where rn<>1
            ''',audit_name
                   )

    #updating MapExpection Table, Map that has been set as active but not loaded since 2022
    dst_cursor.execute(R'''
            update a set NotInTrakingMaps=1
            from MapException a
            where Replace(mapfolder+'\'+mapname,'Y:\','\\ccaintranet.com\dfs-dc-01\')  not in (
            select Replace(Replace(Mapname,'Y:\','\\ccaintranet.com\dfs-dc-01\'),'map.xml','tf.xml') from [RcaDPTeam].[dbo].[MapsInTrackerLoadLog] 
            ) and AuditName=?
                       ''',audit_name
        )

    dst_connection.commit()
    dst_cursor.close()
    dst_connection.close()


else:
    print("   Tracking Maps Table is not Available in this Database")


cursor.close()
connection.close()
