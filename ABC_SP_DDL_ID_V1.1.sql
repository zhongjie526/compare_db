/****** Object:  StoredProcedure [AUDIT_ID].[USP_BalanceCheck]    Script Date: 8/10/2020 7:50:49 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_BalanceCheck'
)
   DROP PROCEDURE AUDIT_ID.USP_BalanceCheck
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_BalanceCheck] @JobRunId [BIGINT],@LBUName [VARCHAR](100) AS
BEGIN
	DECLARE @BalThresholdId bigint,@RecordCount INT,@SourceTablesList [VARCHAR](100), @AmountSrc [VARCHAR](100), @TgtTableName [VARCHAR](100), @AmountTgt [VARCHAR](100), @BalName [VARCHAR](100), @PrimaryKey [VARCHAR] (3000)
	SET @BalThresholdId = 0;
	SET @RecordCount = 0;
	
	DECLARE @BatchRunId bigint
	DECLARE @BatchMasterId bigint, @BalRunInstanceId int, @BalResult int, @JobMasterId BIGINT
	SELECT @BatchRunId=[BATCH_RUN_ID], @JobMasterId=[JOB_MASTER_ID] FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE [JOB_RUN_ID] = @JobRunId
	SELECT @BatchMasterId=[BATCH_MASTER_ID] FROM [AUDIT_ID].[ETL_AUDIT_BATCH_RUN_DETAILS] WHERE [BATCH_RUN_ID] = @BatchRunId
	--PRINT @JobRunId
	while(1=1)
	BEGIN
		--GET RECORD COUNT
		SELECT @RecordCount=COUNT(*) 
		FROM [AUDIT_ID].[ETL_BALANCE_CHECK_ERROR_THRESHOLD]
		WHERE [BAL_CHECK_ERR_THRESHOLD_ID]>@BalThresholdId
		AND [JOB_MASTER_ID] = @JobMasterId
		--PRINT @RecordCount
		--print @BalThresholdId
		--EXIT IF THERE ARE NO MORE RECORDS
		--IF @@ROWCOUNT=0 BREAK;
		IF @RecordCount = 0 BREAK;
		
		--SELECT NEXT RECORD
		SELECT TOP 1 @BalThresholdId = [BAL_CHECK_ERR_THRESHOLD_ID],
			@BalName = [BAL_NAME],
			@PrimaryKey = [TGT_PRIMARY_KEY]
		FROM [AUDIT_ID].[ETL_BALANCE_CHECK_ERROR_THRESHOLD]
		WHERE [BAL_CHECK_ERR_THRESHOLD_ID]>@BalThresholdId
		AND [JOB_MASTER_ID] = @JobMasterId
		ORDER BY [BAL_CHECK_ERR_THRESHOLD_ID]

		--RUN BAL FOR CURRENT TARGET TABLE/COLUMN COMBINATION
		PRINT @BalThresholdId
		--print @JobRunId
		IF @BalName = 'KPI Check'
		BEGIN
			PRINT 'KPI Check'
			EXECUTE [AUDIT_ID].[USP_KPISumCheck] 
			   @BatchRunId=@BatchRunId
			  ,@JobRunId=@JobRunId
			  ,@JobMasterId=@JobMasterId
			  ,@LBUName=@LBUName
			  ,@BalThresholdId=@BalThresholdId
			  ,@PrimaryKey=@PrimaryKey
			  --GO*/
			  --EXECUTE [AUDIT_ID].[USP_KPISumCheck] @BatchRunId,@JobRunId,@JobMasterId,@LBUName,@BalThresholdId 
			  
		END
		IF  @BalName = 'Record Count Check'
		BEGIN
			PRINT 'Record Count Check'
			EXECUTE AUDIT_ID.[USP_CountCheck] 
			   @BatchRunId = @BatchRunId
			  ,@JobRunId = @JobRunId
			  ,@JobMasterId = @JobMasterId
			  ,@LBUName = @LBUName
			  ,@BalThresholdId=@BalThresholdId
			  --GO
			
		END
	END
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_BalanceCheck_STAG]    Script Date: 8/10/2020 7:50:50 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_BalanceCheck_STAG'
)
   DROP PROCEDURE AUDIT_ID.USP_BalanceCheck_STAG
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_BalanceCheck_STAG] @JobRunId [BIGINT],@LBUName [VARCHAR](100) AS
BEGIN
	DECLARE @BalThresholdId bigint,@RecordCount INT,@SrcTablesList [VARCHAR](100), @AmountSrc [VARCHAR](100), @TgtTableName [VARCHAR](100), @AmountTgt [VARCHAR](100), @BalName [VARCHAR](100), @PrimaryKey [VARCHAR] (3000)
	SET @BalThresholdId = 0
	SET @RecordCount = 0
	
	DECLARE @BatchRunId bigint, @BatchMasterId bigint, @BalRunInstanceId int, @BalResult int, @JobMasterId BIGINT
	SELECT @BatchRunId=[BATCH_RUN_ID], @JobMasterId=[JOB_MASTER_ID] FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE [JOB_RUN_ID] = @JobRunId
	SELECT @BatchMasterId=[BATCH_RUN_ID] FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE [BATCH_RUN_ID] = @BatchRunId

	while(1=1)
	BEGIN
		--GET RECORD COUNT
		SELECT @RecordCount=COUNT(*) 
		FROM [AUDIT_ID].[ETL_BALANCE_CHECK_ERROR_THRESHOLD]
		WHERE [BAL_CHECK_ERR_THRESHOLD_ID]>@BalThresholdId
		AND [JOB_MASTER_ID] = @JobMasterId

		--EXIT IF THERE ARE NO MORE RECORDS
		--IF @@ROWCOUNT=0 BREAK;
		IF @RecordCount = 0 BREAK;
		
		--SELECT NEXT RECORD
		SELECT TOP 1 @BalThresholdId = [BAL_CHECK_ERR_THRESHOLD_ID],
					@SrcTablesList = [SRC_TABLE_NAME],
					@AmountSrc = [SRC_COLUMN_NAME],
					@TgtTableName = [TGT_TABLE_NAME],
					@AmountTgt = [TGT_COLUMN_NAME],
					@BalName = [BAL_NAME],
					@PrimaryKey = [TGT_PRIMARY_KEY]
		FROM [AUDIT_ID].[ETL_BALANCE_CHECK_ERROR_THRESHOLD]
		WHERE [BAL_CHECK_ERR_THRESHOLD_ID]>@BalThresholdId
		AND [JOB_MASTER_ID] = @JobMasterId
		ORDER BY [BAL_CHECK_ERR_THRESHOLD_ID]

		--RUN BAL FOR CURRENT TARGET TABLE/COLUMN COMBINATION
		PRINT @BalThresholdId
		IF @BalName = 'KPI Check'
		BEGIN
			PRINT 'KPI Check'
			EXECUTE [AUDIT_ID].[USP_KPISumCheck_Stag] 
			   @BatchRunId=@BatchRunId
			  ,@JobRunId=@JobRunId
			  ,@JobMasterId=@JobMasterId
			  ,@LBUName=@LBUName
			  ,@BalThresholdId=@BalThresholdId
			  ,@PrimaryKey=@PrimaryKey
		END
		IF  @BalName = 'Record Count Check'
		BEGIN
			PRINT 'Record Count Check'
		END
	END
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_BatchFailure]    Script Date: 8/10/2020 7:50:51 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_BatchFailure'
)
   DROP PROCEDURE AUDIT_ID.USP_BatchFailure
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_BatchFailure] @BatchRunId [INT],@LBUName [VARCHAR](100) AS
BEGIN

Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime, @OffsetMultipler int

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

--Use Multipler to decide if we should add or subtract
--print Substring(@offset,1,1)
if Substring(@offset,1,1) = '+'
begin 
	set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
	set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

    SET NOCOUNT ON   
       update AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS
                 SET  [EXECUTION_STATUS] = 'FAILED',
                 [GMT_END_DTTM] = GETDATE(),
                 [LBU_END_DTTM] = @LBUTime
                 WHERE [BATCH_RUN_ID] = @BatchRunId
END


GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_BatchHeaderStart]    Script Date: 8/10/2020 7:50:52 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_BatchHeaderStart'
)
   DROP PROCEDURE AUDIT_ID.USP_BatchHeaderStart
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_BatchHeaderStart] @BatchDate [DATE],@LBUName [varchar](100) AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

	DECLARE @Count datetime;
	Select @Count=count(*)
	from AUDIT_ID.[ETL_AUDIT_BATCH_RUN_HEADER]
	WHERE [BATCH_DT] = @BatchDate
	AND [END_DTTM] IS NULL
       
	Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime,@OffsetMultipler int

	Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

	select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

	--Use Multipler to decide if we should add or subtract
	if Substring(@offset,1,1) = '+'
	begin 
		set @OffsetMultipler = 1
	end
	if Substring(@offset,1,1) = '-'
	begin
		set @OffsetMultipler = -1
	end

	set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
	set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

	Select @LBUTime=DATEADD(Hour,@offset_Hour,GetUTCdate())

	Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

	IF(@Count = 0)
	BEGIN
		Insert into [AUDIT_ID].[ETL_AUDIT_BATCH_RUN_HEADER]
		(
			[BATCH_DT]
			,[START_DTTM]
		)
		SELECT
		@BatchDate,
		@LBUTime
		WHERE 0=(SELECT COUNT(*) FROM [AUDIT_ID].[ETL_AUDIT_BATCH_RUN_HEADER] WHERE [BATCH_DT] = @BatchDate AND [END_DTTM] IS NULL)
	END  
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_BatchHeaderSuccess]    Script Date: 8/10/2020 7:50:53 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_BatchHeaderSuccess'
)
   DROP PROCEDURE AUDIT_ID.USP_BatchHeaderSuccess
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_BatchHeaderSuccess] @BatchDate [DATE],@LBUName [varchar](100) AS
BEGIN

	Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime,@OffsetMultipler int
	Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName
	select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

	if Substring(@offset,1,1) = '+'
	begin 
		set @OffsetMultipler = 1
	end
	if Substring(@offset,1,1) = '-'
	begin
		set @OffsetMultipler = -1
	end

	set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
	set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

	Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

	Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

    SET NOCOUNT ON
	update [AUDIT_ID].[ETL_AUDIT_BATCH_RUN_HEADER]
	SET [END_DTTM] = @LBUTime
	WHERE [BATCH_DT] = @BatchDate
	AND [END_DTTM] IS NULL
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_BatchStart]    Script Date: 8/10/2020 7:50:53 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_BatchStart'
)
   DROP PROCEDURE AUDIT_ID.USP_BatchStart
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_BatchStart] @BatchMasterId [INT],@LBUName [VARCHAR](100),@BatchDate [DATE],@BatchRunId [BIGINT] OUT AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
       DECLARE @AcctPeriod [VARCHAR](10),@ExecutionStatus VARCHAR(100),@MaxBatchRunId INT
       Select @ExecutionStatus=[EXECUTION_STATUS], @MaxBatchRunId=BATCH_RUN_ID from AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS
       WHERE BATCH_RUN_ID = (SELECT MAX(BATCH_RUN_ID) FROM AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS WHERE BATCH_MASTER_ID= @BatchMasterId)
SELECT @AcctPeriod = FORMAT(@BatchDate,'yyyy') + '0' + FORMAT(@BatchDate,'MM')
       
Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime,@OffsetMultipler int

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

--Use Multipler to decide if we should add or subtract
--print Substring(@offset,1,1)
if Substring(@offset,1,1) = '+'
begin 
	set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
	set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler
--print @offset_Hour
--print @offset_Minute
Select @LBUTime=DATEADD(Hour,@offset_Hour,GetUTCdate())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)
       --DECLARE @BatchStartDateTime [DateTime];
       --SET @BatchStartDateTime=getdate()

       IF(@ExecutionStatus='RUN')
       BEGIN
       SET @BatchRunId=-1
       END
       ELSE IF(@ExecutionStatus='FAILED')
       BEGIN
       SET @BatchRunId=@MaxBatchRunId
       UPDATE AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS SET EXECUTION_STATUS='RUN', LBU_END_DTTM=null, GMT_END_DTTM=null
       WHERE BATCH_RUN_ID=@MaxBatchRunId
       END
       ELSE
       BEGIN
 	   DECLARE @maxValue int
	   DECLARE @loopState int
	   set @loopState = 0
	   while @loopState = 0
	   BEGIN
       SELECT @maxValue = MAX(BATCH_RUN_ID) from AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS
       if @maxValue < 1 or @maxValue IS NULL
            set @maxValue = 0
       --SET IDENTITY_INSERT AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS ON;
	   
       Insert into AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS
       ([BATCH_RUN_ID],
	   [BATCH_MASTER_ID]
       --,[LBU_CODE]
	   --,[LBU_NAME]
	   ,[BATCH_DT]
	   ,[ACCT_PERIOD]
           ,[LBU_START_DTTM]
           ,[GMT_START_DTTM]
           ,[EXECUTION_STATUS])
       SELECT
              @maxValue + 1,
			  @BatchMasterId
              --,@LBUCode
              --,@LBUName
			  ,@BatchDate
			  ,@AcctPeriod
              ,@LBUTime
              ,GetUTCdate()
              ,'RUN'
	  WHERE 0=(SELECT COUNT(*) FROM AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS WHERE BATCH_RUN_ID = @maxValue + 1)
      -- SET @BatchRunId=(SELECT MAX(BATCH_RUN_ID) FROM AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS)
	   
	  --SET IDENTITY_INSERT AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS OFF;
		SELECT @loopState = COALESCE(MAX(BATCH_RUN_ID),0) from AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS WHERE BATCH_MASTER_ID = @BatchMasterId AND BATCH_RUN_ID = @maxValue + 1
	   END
       END
    
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_BatchSuccess]    Script Date: 8/10/2020 7:50:54 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_BatchSuccess'
)
   DROP PROCEDURE AUDIT_ID.USP_BatchSuccess
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_BatchSuccess] @BatchMasterId [INT],@LBUName [VARCHAR](100) AS
BEGIN
Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime,@OffsetMultipler int
DECLARE @BatchRunId int, @JobRunId INT, @BalResult varchar(20)


SELECT TOP 1 @BatchRunId = BATCH_RUN_ID FROM [AUDIT_ID].[ETL_AUDIT_BATCH_RUN_DETAILS] WHERE BATCH_MASTER_ID = @BatchMasterId ORDER BY LBU_START_DTTM DESC
--SELECT @JobRunId= TOP 1 JOB_RUN_ID FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE BATCH_RUN_ID = @BatchRunId ORDER BY LBU_START_DTTM DESC

PRINT CONVERT(VARCHAR,@BatchRunId)

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone
--Use Multipler to decide if we should add or subtract
--print Substring(@offset,1,1)
if Substring(@offset,1,1) = '+'
begin 
	set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
	set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

    SET NOCOUNT ON
	Select TOP 1 @BalResult = BAL_STATUS FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS WHERE BAL_STATUS = 'FAILURE' 
	AND JOB_RUN_ID IN ( SELECT JOB_RUN_ID FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE BATCH_RUN_ID = @BatchRunId );

	  IF @BalResult = 'FAILURE' 
	  EXECUTE AUDIT_ID.[USP_BatchFailure] @BatchRunId = @BatchRunId, @LBUName = @LBUName
	  ELSE
       update AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS
                 SET  [EXECUTION_STATUS] ='SUCCESSFUL',
                 [GMT_END_DTTM] = GETDATE(),
                 [LBU_END_DTTM] = @LBUTime
                 WHERE [BATCH_RUN_ID] = @BatchRunId
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_CountCheck]    Script Date: 8/10/2020 7:50:55 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_CountCheck'
)
   DROP PROCEDURE AUDIT_ID.USP_CountCheck
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_CountCheck] @BatchRunId [BIGINT],@JobRunId [BIGINT],@JobMasterId [BIGINT],@LBUName [VARCHAR](100),@BalThresholdId [INT] AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    DECLARE @SourceTablesList varchar(2000);
    DECLARE @TgtTableName varchar(50);
	DECLARE @SourceFilterCondition VARCHAR(2000);
	DECLARE @SourceColumnName VARCHAR(2000);
    DECLARE @TgtColumnName VARCHAR(2000);
    DECLARE @TargetFilterCondition VARCHAR(2000);
	DECLARE @SourceCount BIGINT;
	DECLARE @TargetCount BIGINT;
    DECLARE @CurrentCount BIGINT;
	DECLARE @ErrorCount DECIMAL(38,10);
	DECLARE @tgtstr NVARCHAR(2000);
	DECLARE @errstr NVARCHAR(2000);
	DECLARE @ErrorThreshold INT;
    DECLARE @ThresholdId INT;
	DECLARE @PctOrValue VARCHAR(50);
    DECLARE @ParmDefinition1 nvarchar(500);
	DECLARE @ParmDefinition2 nvarchar(500);
	DECLARE @ParmDefinition3 nvarchar(500);
	DECLARE @BalRunInstanceId [INT],@BalResult [VARCHAR](100);

    -- Set the parameters for the sp_executesql statement
	SET @ParmDefinition1 = N'@TargetCountOUT BIGINT OUTPUT';
	SET @ParmDefinition2 = N'@CurrentCountOUT BIGINT OUTPUT';
	SET @ParmDefinition3 = N'@ErrorCountOUT BIGINT OUTPUT';   

	  SELECT @SourceTablesList =  SRC_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @SourceColumnName =  SRC_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @TgtTableName =  TGT_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @TgtColumnName =  TGT_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;
	  
	  SELECT @SourceFilterCondition =  SRC_FILTER_CONDITION FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @TargetFilterCondition =  TGT_FILTER_CONDITION FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	
	print LEN(@SourceTablesList)
       WHILE LEN(@SourceTablesList) > 0
      BEGIN
             DECLARE @CurrentTableName varchar(1000);
			 DECLARE @currentstr NVARCHAR(2000);

             IF CHARINDEX(',',@SourceTablesList) > 0
                    SET  @CurrentTableName = SUBSTRING(@SourceTablesList,0,CHARINDEX(',',@SourceTablesList))
             ELSE
                    BEGIN
                    SET  @CurrentTableName = @SourceTablesList
                    SET @SourceTablesList = ''
                    END
	
	PRINT @SourceTablesList;
	SET @currentstr = N'SELECT @CurrentCountOUT = COUNT(' +@SourceColumnName+ ') FROM ' + @CurrentTableName + 
	' WHERE BATCH_RUN_ID = (SELECT MAX(BATCH_RUN_ID) FROM ' + @CurrentTableName + ') AND JOB_RUN_ID = (SELECT MAX(JOB_RUN_ID) FROM ' + @CurrentTableName +') AND ' + @SourceFilterCondition
	print @currentstr;


	EXECUTE sp_executesql
     @currentstr
    ,@ParmDefinition2
    ,@CurrentCountOUT = @CurrentCount OUTPUT;

	SET @SourceCount = COALESCE(@SourceCount,0) + COALESCE(@CurrentCount,0);

	print @SourceCount;

	SET @SourceTablesList = REPLACE(@SourceTablesList,@CurrentTableName + ',' , '')

	END


       SET @tgtstr = N'SELECT @TargetCountOUT = COUNT(' +@TgtColumnName+ ') 
       FROM ' + @TgtTableName + ' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ' AND ' + @TargetFilterCondition

	PRINT @tgtstr;

	  Select @ErrorThreshold=ERROR_THRESHOLD FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;          
	  Select @PctOrValue=PCT_OR_VALUE FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check'      

    -- Execute the SQL String using sp_executesql, passing in the parameter definition and defining the output variable

	EXECUTE sp_executesql
     @tgtstr
    ,@ParmDefinition1
    ,@TargetCountOUT = @TargetCount OUTPUT;

   SET @errstr = N'SELECT @ErrorCountOUT = COUNT(DISTINCT ERROR_RECORD_PRIMARY_KEY_VALUES) 
       FROM AUDIT_ID.[ETL_ERROR_LOG] WITH (NOLOCK) 
       WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar);

	   print @errstr;


 EXECUTE sp_executesql
    @errstr
    ,@ParmDefinition3
    ,@ErrorCountOUT = @ErrorCount OUTPUT;

	PRINT COALESCE(@SourceCount,0);
	PRINT COALESCE(@TargetCount,0);
	PRINT COALESCE(@ErrorCount,0);

Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@OffsetMultipler int, @LBUTime DateTime

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

if Substring(@offset,1,1) = '+'
begin
    set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
    set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

       /* GET BalanceRunInstanceId*/

	  /*IF ((CASE WHEN (COALESCE(@SourceCount,0) = COALESCE(@TargetCount,0) and COALESCE(@ErrorCount,0) = 0)
	  THEN 0
	  WHEN (COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0) AND COALESCE(@ErrorCount,0) = 0)
	  THEN ((CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
	  WHEN (COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0) AND COALESCE(@ErrorCount,0) = 0)
	  THEN ((CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@TargetCount,0) AS FLOAT)) * 100
	  WHEN (COALESCE(@SourceCount,0) <> COALESCE(@TargetCount,0) AND COALESCE(@SourceCount,0) >= COALESCE(@ErrorCount,0) )
	  THEN 100 - ((CAST(COALESCE(@SourceCount,0) - COALESCE(@ErrorCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
      WHEN (COALESCE(@SourceCount,0) <> COALESCE(@TargetCount,0) AND COALESCE(@ErrorCount,0) > COALESCE(@SourceCount,0))
      THEN 100 - ((CAST(COALESCE(@ErrorCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@ErrorCount,0) AS FLOAT)) * 100 
	  ELSE 0 END) <= COALESCE(@ErrorThreshold,0)) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'*/

	  	  IF ((CASE WHEN (COALESCE(@SourceCount,0) = COALESCE(@TargetCount,0))
	  THEN 0
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@TargetCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))
	  ELSE 0 END) <= COALESCE(@ErrorThreshold,0)) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'
	 
	 PRINT @BalResult

	 /*
	    IF ((CASE WHEN (COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0) )
			   THEN ((CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
			   WHEN (COALESCE(@TargetCount,0) > COALESCE(@SourceCount,0))
			   THEN ((CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@TargetCount,0) AS FLOAT)) * 100 
			   ELSE 0 END) <= COALESCE(@ErrorThreshold,0) OR @SourceCount = @TargetCount) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'
	   */
	   
	   SELECT @BalRunInstanceId=MAX(BAL_RUN_INSTANCE_ID) FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS
       BEGIN
	   /*DECLARE @maxValue int;
	   SELECT @maxValue = MAX(BAL_RUN_INSTANCE_ID) from AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
	   if @maxValue < 1 or @maxValue IS NULL
		set @maxValue = 0
	   SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] ON;*/
       Insert into AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
       (--[BAL_RUN_INSTANCE_ID],
	    [JOB_RUN_ID],
        [BAL_CHECK_ERR_THRESHOLD_ID],
        [BAL_NAME],
        [SRC_BAL_VALUE],
        [TGT_BAL_VALUE],
        [ERROR_BAL_VALUE],
        [ERROR_PCT_OR_VALUE],
        [BAL_STATUS],
        [LBU_CREATED_DTTM],
        [GMT_CREATED_DTTM])
       SELECT
             -- @maxValue + 1,
			  @JobRunId
              ,@BalThresholdId
              ,'Record Count Check'
              ,COALESCE(@SourceCount,0)
              ,COALESCE(@TargetCount,0)
              ,COALESCE(@ErrorCount,0)
              ,CASE WHEN (COALESCE(@SourceCount,0) = COALESCE(@TargetCount,0))
	  THEN 0
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@TargetCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))
	  ELSE 0 END
              ,@BalResult
              ,GetUTCdate()
              ,@LBUTime  
	  -- SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] OFF;
       END
END 

GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_CountCheck_Stag]    Script Date: 8/10/2020 7:50:56 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_CountCheck_Stag'
)
   DROP PROCEDURE AUDIT_ID.USP_CountCheck_Stag
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_CountCheck_Stag] @BatchRunId [BIGINT],@JobRunId [BIGINT],@JobMasterId [BIGINT],@LBUName [VARCHAR](100),@BalRunInstanceId [INT] OUT,@BalResult [VARCHAR](100) OUT AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
	DECLARE @srctablename VARCHAR(2000);
	DECLARE @srccolumnname VARCHAR(100);
    DECLARE @tgttablename VARCHAR(50);
	DECLARE @TgtColumnName VARCHAR(100);
    DECLARE @SourceCount DECIMAL(38,10);
    DECLARE @TargetCount DECIMAL(38,10);
    DECLARE @ErrorCount DECIMAL(38,10);
	DECLARE @ErrorThreshold INT;
    DECLARE @ThresholdId INT;
	DECLARE @PctOrValue VARCHAR(50);
    DECLARE @srcstr NVARCHAR(2000);
    DECLARE @tgtstr NVARCHAR(2000);
    DECLARE @errstr NVARCHAR(2000);
    DECLARE @ParmDefinition1 nvarchar(500);
    DECLARE @ParmDefinition2 nvarchar(500);
    DECLARE @ParmDefinition3 nvarchar(500);

    -- Set the parameters for the sp_executesql statement
    SET @ParmDefinition1 = N'@SourceCountOUT BIGINT OUTPUT';
    SET @ParmDefinition2 = N'@TargetCountOUT BIGINT OUTPUT';
    SET @ParmDefinition3 = N'@ErrorCountOUT BIGINT OUTPUT';    

	  SELECT @srctablename =  SRC_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check';

	  SELECT @srccolumnname =  SRC_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' ;

	  SELECT @tgttablename =  TGT_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check' ;

	  SELECT @TgtColumnName =  TGT_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check';

    SET @srcstr = N'SELECT DISTINCT @SourceCountOUT = ' + @srccolumnname + ' FROM ' + @srctablename + ' WITH (NOLOCK) WHERE CONTROL_FILE_ID IN (SELECT MAX(CONTROL_FILE_ID) FROM '  + @srctablename + ' WHERE STAG_JOB_MASTER_ID = ' + CAST(@JobMasterId AS nvarchar) + ') AND STAG_JOB_MASTER_ID = ' + CAST(@JobMasterId AS nvarchar);
	print @srcstr;
    
	SET @tgtstr = N'SELECT @TargetCountOUT = COUNT(' +@TgtColumnName+ ') 
       FROM ' + @tgttablename + ' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar);

	print @tgtstr;

    SET @errstr = N'SELECT @ErrorCountOUT = COUNT(DISTINCT ERROR_RECORD_PRIMARY_KEY_VALUES) 
       FROM AUDIT_ID.[ETL_ERROR_LOG] WITH (NOLOCK) 
       WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar);

	   print @errstr;

  Select @ErrorThreshold=ERROR_THRESHOLD FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check'      
  Select @ThresholdId=BAL_CHECK_ERR_THRESHOLD_ID FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check'  
  Select @PctOrValue=PCT_OR_VALUE FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check'      


    -- Execute the SQL String using sp_executesql, passing in the parameter definition and defining the output variable
    EXECUTE sp_executesql
    @srcstr
    ,@ParmDefinition1
    ,@SourceCountOUT = @SourceCount OUTPUT;

       EXECUTE sp_executesql
    @tgtstr
    ,@ParmDefinition2
    ,@TargetCountOUT = @TargetCount OUTPUT;

       EXECUTE sp_executesql
    @errstr
    ,@ParmDefinition3
    ,@ErrorCountOUT = @ErrorCount OUTPUT;

	PRINT COALESCE(@SourceCount,0);
	PRINT COALESCE(@TargetCount,0);
	PRINT COALESCE(@ErrorCount,0);

Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10), @OffsetMultipler int, @LBUTime DateTime

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

if Substring(@offset,1,1) = '+'
begin
    set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
    set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

/*
	  IF ((CASE WHEN (COALESCE(@SourceCount,0) = COALESCE(@TargetCount,0) and COALESCE(@ErrorCount,0) = 0)
	  THEN 0
	  WHEN (COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0) AND COALESCE(@ErrorCount,0) = 0)
	  THEN ((CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
	  WHEN (COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0) AND COALESCE(@ErrorCount,0) = 0)
	  THEN ((CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@TargetCount,0) AS FLOAT)) * 100
	  WHEN (COALESCE(@SourceCount,0) <> COALESCE(@TargetCount,0) AND COALESCE(@SourceCount,0) >= COALESCE(@ErrorCount,0) )
	  THEN 100 - ((CAST(COALESCE(@SourceCount,0) - COALESCE(@ErrorCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
      WHEN (COALESCE(@SourceCount,0) <> COALESCE(@TargetCount,0) AND COALESCE(@ErrorCount,0) > COALESCE(@SourceCount,0))
      THEN 100 - ((CAST(COALESCE(@ErrorCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@ErrorCount,0) AS FLOAT)) * 100 
	  ELSE 0 END) <= COALESCE(@ErrorThreshold,0)) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'
	 
	 */

	  IF ((CASE WHEN (COALESCE(@SourceCount,0) = COALESCE(@TargetCount,0))
	  THEN 0
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@TargetCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))
	  ELSE 0 END) <= COALESCE(@ErrorThreshold,0)) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'
	 
	 PRINT @BalResult
	 

	  SELECT @BalRunInstanceId=MAX(BAL_RUN_INSTANCE_ID) FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS
       BEGIN
         /* DECLARE @maxValue int;
          SELECT @maxValue = MAX(BAL_RUN_INSTANCE_ID) from AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
          if @maxValue < 1 or @maxValue IS NULL
              set @maxValue = 0
          SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] ON;*/
       Insert into AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
       (--[BAL_RUN_INSTANCE_ID],
        [JOB_RUN_ID],
        [BAL_CHECK_ERR_THRESHOLD_ID],
        [BAL_NAME],
        [SRC_BAL_VALUE],
        [TGT_BAL_VALUE],
        [ERROR_BAL_VALUE],
        [ERROR_PCT_OR_VALUE],
        [BAL_STATUS],
        [LBU_CREATED_DTTM],
        [GMT_CREATED_DTTM])
       SELECT
              -- @maxValue+1,
              @JobRunId
              ,@ThresholdId
              ,'Record Count Check'
              ,COALESCE(@SourceCount,0)
              ,COALESCE(@TargetCount,0)
              ,COALESCE(@ErrorCount,0)
              ,CASE WHEN (COALESCE(@SourceCount,0) = COALESCE(@TargetCount,0))
	  THEN 0
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))/CAST(COALESCE(@SourceCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN ((CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))/CAST(COALESCE(@TargetCount,0) AS FLOAT)) * 100
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) > COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@SourceCount,0) - COALESCE(@TargetCount,0) AS FLOAT))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceCount,0) < COALESCE(@TargetCount,0))
	  THEN (CAST(COALESCE(@TargetCount,0) - COALESCE(@SourceCount,0) AS FLOAT))
	  ELSE 0 END
			   ,@BalResult
              ,GetUTCdate()
              ,@LBUTime  
         -- SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] OFF;
       --SET @BalRunInstanceId=(SELECT MAX(BAL_RUN_INSTANCE_ID) FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS) 
       END
END 
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_DatatypeLengthCheck]    Script Date: 8/10/2020 7:50:57 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_DatatypeLengthCheck'
)
   DROP PROCEDURE AUDIT_ID.USP_DatatypeLengthCheck
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_DatatypeLengthCheck] @TableName [varchar](50),@TgtTableName [varchar](50),@ValidationColumns [VARCHAR](500),@BatchName [VARCHAR](50),@SSISPackageName [VARCHAR](1000) AS
BEGIN


declare @testsql varchar(max);
declare @testtable varchar(max);
DECLARE @BatchRunId BIGINT;
DECLARE @JobRunId BIGINT;



SELECT @JobRunId = MAX(JOB_RUN.JOB_RUN_ID), @BatchRunId = MAX(JOB_RUN.BATCH_RUN_ID)
FROM AUDIT_ID.[ETL_AUDIT_JOB_RUN_DETAILS] JOB_RUN
WHERE JOB_MASTER_ID = (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              )
       );

	   PRINT @BatchRunId;
	   PRINT @JobRunId

SET @testtable = 'testtable_' + CAST(@BatchRunId AS NVARCHAR)+'_'+ CAST(@JobRunId AS NVARCHAR);


SET @testsql = 'IF Object_id('''+@testtable+''') IS NOT NULL 
  BEGIN 
      DROP TABLE ' +@testtable+'; 
  END ;';


  SET @testsql = @testsql + 'CREATE TABLE '+@testtable+
  ' (
srccol varchar(500),
tgtcol varchar(500),
coldatatype varchar(500),
collength int,
RN int
	 );';


set @TgtTableName = replace(replace(@TgtTableName,'[',''),']','');
set @TableName =  replace(replace(@TableName,'[',''),']','');
SET @ValidationColumns = REPLACE(@ValidationColumns,',',''',''');

PRINT @TgtTableName;
PRINT @TableName;
PRINT @ValidationColumns;


set @testsql = @testsql + 'INSERT INTO ' + @testtable + '  (srccol,tgtcol,coldatatype,collength,RN)
SELECT b.COLUMN_NAME as srccol, a.COLUMN_NAME as tgtcol, a.DATA_TYPE as coldatatype, a.CHARACTER_MAXIMUM_LENGTH as collength,ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS RN  FROM
(SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS RN 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA+''.''+TABLE_NAME = ''' + @TgtTableName + ''' AND COLUMN_NAME IN (''' + @ValidationColumns + ''')
) a 
INNER JOIN 
(SELECT COLUMN_NAME,ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS RN 
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ''' + @TableName + ''' AND COLUMN_NAME IN (''' + @ValidationColumns + ''')) b
ON a.RN = b.RN'


PRINT @testsql;
exec (@testsql);



declare @id int =1;
declare @sql nvarchar(max);
DECLARE @Cnt int;
DECLARE @ColumnDataType VARCHAR(MAX);
DECLARE @ColumnDataLength VARCHAR(MAX);
DECLARE @ParmDefinition1 nvarchar(MAX);
DECLARE @cntstr NVARCHAR(MAX);


SET @ParmDefinition1 = N'@CntOUT BIGINT OUTPUT';

SET @cntstr = N'SELECT @CntOUT = count(1) from ' + @testtable;

print @cntstr


	EXECUTE sp_executesql
     @cntstr
    ,@ParmDefinition1
    ,@CntOUT = @Cnt OUTPUT;

PRINT @Cnt

while @id <= @Cnt
begin

SET @sql = '';

DECLARE @ParmDefinition2 NVARCHAR(500);
DECLARE @valcolstr NVARCHAR(2000);

SET @ParmDefinition2 = N'@valcolOUT VARCHAR(MAX) OUTPUT';

SET @valcolstr = N'SELECT @valcolOUT =   tgtcol from ' + @testtable + '  where RN = ' + CAST(@id AS NVARCHAR);
	
	PRINT @valcolstr;
	
	EXECUTE sp_executesql
     @valcolstr
    ,@ParmDefinition2
    ,@valcolOUT = @ValidationColumns OUTPUT;

	print @ValidationColumns;

DECLARE @ParmDefinition3 NVARCHAR(500);
DECLARE @valcoldtstr NVARCHAR(2000);

SET @ParmDefinition3 = N'@valcoldatatypeOUT VARCHAR(MAX) OUTPUT';

SET @valcoldtstr = N'SELECT @valcoldatatypeOUT =   coldatatype from ' + @testtable + '  where RN = ' + CAST(@id AS NVARCHAR);
	
	EXECUTE sp_executesql
     @valcoldtstr
    ,@ParmDefinition3
    ,@valcoldatatypeOUT = @ColumnDataType OUTPUT;


DECLARE @ParmDefinition4 NVARCHAR(500);
DECLARE @valcollenstr NVARCHAR(2000);

SET @ParmDefinition4 = N'@valcollenOUT BIGINT OUTPUT';

SET @valcollenstr = N'SELECT @valcollenOUT =   collength from ' + @testtable + '  where RN = ' + CAST(@id AS NVARCHAR);

	
	EXECUTE sp_executesql
     @valcollenstr
    ,@ParmDefinition4
    ,@valcollenOUT = @ColumnDataLength OUTPUT;

	if UPPER(@ColumnDataType) = 'CHAR' OR UPPER(@ColumnDataType) = 'VARCHAR'
	begin
			set @sql = N'INSERT INTO AUDIT_ID.[ETL_ERROR_LOG]
           ([BATCH_RUN_ID]
           ,[JOB_RUN_ID]
           ,[ERROR_REF_CD]
           ,[ERROR_RECORD_PRIMARY_KEY_VALUES]
           ,[ERROR_DTTM]
           ,[UPDATED_DTTM]
		   ,[PKG_NAME]
           ,[ERROR_COLUMNS]
           ,[ERROR_VALUE])
		SELECT '  + CAST(@BatchRunId AS VARCHAR) + ', '
          + CAST(@JobRunId AS VARCHAR) + ',
          9000003,
          ''Row '' + CAST(A.RN AS VARCHAR(15)),
          GETDATE(),
          GETDATE(),'''
		  + @SSISPackageName + ''',
          '''+ @ValidationColumns + ''',
	      ' + CHAR(39) + 'String is too big' + CHAR(39) + '
		   FROM (
				SELECT '+@ValidationColumns+', ROW_NUMBER() OVER (order by (SELECT NULL)) AS RN
				FROM [' + @TableName + ']
				) A
		   WHERE LEN(A.'+@ValidationColumns+') > ' + CAST(@ColumnDataLength as varchar) + ';'
	end
	else if UPPER(@ColumnDataType) = 'INT' OR UPPER(@ColumnDataType) = 'BIGINT' OR UPPER(@ColumnDataType) = 'SMALLINT'  OR UPPER(@ColumnDataType) = 'NUMERIC' OR UPPER(@ColumnDataType) = 'DECIMAL'
	begin
		set @sql = N'INSERT INTO AUDIT_ID.[ETL_ERROR_LOG]
           ([BATCH_RUN_ID]
           ,[JOB_RUN_ID]
           ,[ERROR_REF_CD]
           ,[ERROR_RECORD_PRIMARY_KEY_VALUES]
           ,[ERROR_DTTM]
           ,[UPDATED_DTTM]
		   ,[PKG_NAME]
           ,[ERROR_COLUMNS]
           ,[ERROR_VALUE])
SELECT '  + CAST(@BatchRunId AS VARCHAR) + ', '
          + CAST(@JobRunId AS VARCHAR) + ',
          9000003,
          ''Row '' + CAST(A.RN AS VARCHAR(15)),
          GETDATE(),
          GETDATE(),'''
		  + @SSISPackageName + ''',
          '''+ @ValidationColumns + ''',
	      ' + CHAR(39) + 'Not a valid number' + CHAR(39) + '
          FROM (
				SELECT '+@ValidationColumns+', ROW_NUMBER() OVER (order by (SELECT NULL)) AS RN
				FROM [' + @TableName + ']
				) A
		   WHERE LEN(A.'+@ValidationColumns+') <> 0 AND ISNUMERIC(A.'+@ValidationColumns+') = 0;'

	end
	else if UPPER(@ColumnDataType) = 'DATE' OR UPPER(@ColumnDataType) = 'DATETIME' 
	begin

		set @sql = N'INSERT INTO AUDIT_ID.[ETL_ERROR_LOG]
           ([BATCH_RUN_ID]
           ,[JOB_RUN_ID]
           ,[ERROR_REF_CD]
           ,[ERROR_RECORD_PRIMARY_KEY_VALUES]
           ,[ERROR_DTTM]
           ,[UPDATED_DTTM]
		   ,[PKG_NAME]
           ,[ERROR_COLUMNS]
           ,[ERROR_VALUE])
SELECT '  + CAST(@BatchRunId AS VARCHAR) + ', '
          + CAST(@JobRunId AS VARCHAR) + ',
          9000003,
          ''Row '' + CAST(A.RN AS VARCHAR(15)),
          GETDATE(),
          GETDATE(),'''
		  + @SSISPackageName + ''',
          '''+ @ValidationColumns + ''',
	      ' + CHAR(39) + 'Not a valid date/datetime' + CHAR(39) + '
          FROM (
				SELECT '+@ValidationColumns+', ROW_NUMBER() OVER (order by (SELECT NULL)) AS RN
				FROM [' + @TableName + ']
				) A
		   WHERE LEN(A.'+@ValidationColumns+') <> 0 AND ISDATE(A.'+@ValidationColumns+') = 0;'
	end 
	PRINT @sql

	EXEC sp_executesql @sql


set @id = @id + 1

END
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_DQDataCheck]    Script Date: 8/10/2020 7:50:58 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_DQDataCheck'
)
   DROP PROCEDURE AUDIT_ID.USP_DQDataCheck
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_DQDataCheck] @BatchName [VARCHAR](50),@SSISPackageName [VARCHAR](50) AS 
BEGIN

DECLARE @BatchRunId BIGINT;
DECLARE @JobRunId BIGINT;


SELECT @JobRunId = MAX(JOB_RUN.JOB_RUN_ID), @BatchRunId = MAX(JOB_RUN.BATCH_RUN_ID)
FROM AUDIT_ID.[ETL_AUDIT_JOB_RUN_DETAILS] JOB_RUN
WHERE JOB_MASTER_ID = (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              ));

			  PRINT @BatchRunId;
			  PRINT @JobRunId;

DECLARE @RefCheckId bigint, @RecordCount int, @TableName varchar(2000),@PrimaryKey varchar(1000),@ColumnName varchar(1000),@RefTableName varchar(2000),@RefColumnName varchar(1000),@RefColumnValues varchar(2000),@ErrorRefCd bigint;
	
	SET @RefCheckId = 0
	SET @RecordCount = 0


WHILE(1=1)

	BEGIN
		--GET RECORD COUNT
		SELECT @RecordCount=COUNT(*) 
		FROM [AUDIT_ID].[ETL_REF_VALUE_CHECK]
		WHERE [REF_CHECK_ID]>@RefCheckId
		AND [JOB_MASTER_ID] =  (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              ));

		PRINT @RecordCount;

		--EXIT IF THERE ARE NO MORE RECORDS
		--IF @@ROWCOUNT=0 BREAK;
		IF @RecordCount = 0 BREAK;
				
		--SELECT NEXT RECORD
		SELECT TOP 1 @RefCheckId = [REF_CHECK_ID],
					@TableName = TABLE_NAME,
					@ColumnName = COLUMN_NAME,
					@PrimaryKey = PRIMARY_KEY,
					@RefTableName = REF_TABLE_NAME,
					@RefColumnName = REF_COLUMN_NAME,
					@RefColumnValues = REF_COLUMN_VALUES
		FROM AUDIT_ID.[ETL_REF_VALUE_CHECK]
		WHERE [REF_CHECK_ID]>@RefCheckId
		AND [JOB_MASTER_ID] = (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              ))
		ORDER BY [REF_CHECK_ID];

		PRINT @RefCheckId ;
		PRINT @TableName ;
		PRINT @ColumnName ;
		PRINT @PrimaryKey ;
		PRINT @RefTableName ;
		PRINT @RefColumnName;
		PRINT @RefColumnValues;
		
DECLARE @createsql varchar(max), @reftest varchar(max);

SET @reftest = 'reftest_' + CAST(@BatchRunId AS NVARCHAR)+'_'+ CAST(@JobRunId AS NVARCHAR);

PRINT @reftest;

SET @createsql = 'IF Object_id('''+@reftest+''') IS NOT NULL 
  BEGIN 
      DROP TABLE ' +@reftest+'; 
  END ;';

    SET @createsql = @createsql + 'CREATE TABLE '+@reftest+
  ' (column_name varchar(1000),
primarykey varchar(1000),
err_flag varchar(1)
	 );';

PRINT @createsql;
EXEC (@createsql);


SET @PrimaryKey = 'CAST (' + REPLACE(@PrimaryKey,',',' AS VARCHAR(50))+''|''+CAST(') + ' AS VARCHAR(50))';


declare @refsql nvarchar(max), @defsql nvarchar(max);

set @refsql = N'INSERT INTO ' +@reftest+ ' (column_name,primarykey,err_flag)
SELECT ' + @ColumnName + ',' + @PrimaryKey + ', CASE WHEN ' + @ColumnName + ' ' + @RefColumnValues + ' THEN ''N'' ELSE ''Y'' END ERR_FLAG
FROM '  + @TableName +
' AND BATCH_RUN_ID = ' + CAST(@BatchRunId AS VARCHAR) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS VARCHAR) +  
';';


set @defsql = N'INSERT INTO ' +@reftest+ ' (column_name,primarykey,err_flag)
SELECT ' + @ColumnName + ',' + @PrimaryKey + ',''Y'' FROM '  + @TableName +
' AND ' +@ColumnName + ' NOT IN (SELECT DISTINCT ' + @RefColumnName + ' FROM ' +@RefTableName + ') 
AND BATCH_RUN_ID = ' + CAST(@BatchRunId AS VARCHAR) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS VARCHAR) +  
';';

PRINT @refsql;
PRINT @defsql;

IF (@RefColumnValues <> 'NA')
EXEC sp_executesql @refsql;
ELSE 
EXEC sp_executesql @defsql;

PRINT @refsql;
PRINT @defsql;


declare @sql nvarchar(max);

SET @sql = '';

SET @sql = N'INSERT INTO AUDIT_ID.[ETL_ERROR_LOG]
           ([BATCH_RUN_ID]
           ,[JOB_RUN_ID]
           ,[ERROR_REF_CD]
           ,[ERROR_RECORD_PRIMARY_KEY_VALUES]
           ,[ERROR_DTTM]
           ,[UPDATED_DTTM]
		   ,[PKG_NAME]
           ,[ERROR_COLUMNS]
           ,[ERROR_VALUE])
		SELECT ' + CAST(@BatchRunId AS VARCHAR) + ', '
          + CAST(@JobRunId AS VARCHAR) + ', 
		  9000006, 
          primarykey,
		  GETDATE(),
          GETDATE(),
		  ''' + @SSISPackageName + ''', 
          '''+ @ColumnName + ''',
	      --column_name
		  CONCAT('''+ @ColumnName + ' has the value '', column_name) 
		 FROM ' + @reftest + ' WHERE err_flag = ''Y'';'

PRINT @sql;

EXEC sp_executesql @sql;

END 

END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_DuplicateCheck]    Script Date: 8/10/2020 7:50:59 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_DuplicateCheck'
)
   DROP PROCEDURE AUDIT_ID.USP_DuplicateCheck
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_DuplicateCheck] @BatchName [VARCHAR](50),@SSISPackageName [VARCHAR](50) AS 
BEGIN
	
DECLARE @BatchRunId BIGINT ;
DECLARE @JobRunId BIGINT;


SELECT @JobRunId = MAX(JOB_RUN.JOB_RUN_ID), @BatchRunId = MAX(JOB_RUN.BATCH_RUN_ID)
FROM AUDIT_ID.[ETL_AUDIT_JOB_RUN_DETAILS] JOB_RUN
WHERE JOB_MASTER_ID = (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              )
       );
	   


DECLARE @TableName varchar(2000),@PrimaryKey varchar(1000),@PrimaryKeyPipe varchar(1000),@FilterCondition varchar(2000),@ErrorValue varchar(100), @dupsql nvarchar(max),@ParmDefinition3 NVARCHAR(500),@DupCnt INT,@sql nvarchar(max),@Cnt int,@id int =1;

SELECT @TableName =  TABLE_NAME FROM AUDIT_ID.[ETL_DUPLICATE_CHECK] WHERE JOB_MASTER_ID = (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              )       );

SELECT @FilterCondition =  FILTER_CONDITION FROM AUDIT_ID.[ETL_DUPLICATE_CHECK] WHERE JOB_MASTER_ID = (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              ));

SELECT @PrimaryKey =  PRIMARY_KEY FROM AUDIT_ID.[ETL_DUPLICATE_CHECK] WHERE JOB_MASTER_ID = (
       SELECT JOB_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = @SSISPackageName
AND BATCH_MASTER_ID IN (
                     SELECT BATCH_MASTER_ID FROM AUDIT_ID.[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = TRIM(@BatchName)
              )       );

SELECT @ErrorValue = ERROR_SUMMARY FROM AUDIT_ID.[ETL_ERROR_REFERENCE] WHERE ERROR_REF_CD = 9000004

PRINT @TableName;
PRINT @FilterCondition;
PRINT @PrimaryKey;
PRINT @ErrorValue;


declare @createsql varchar(max), @duptest varchar(max);

SET @duptest = 'duptest_' + CAST(@BatchRunId AS NVARCHAR)+'_'+ CAST(@JobRunId AS NVARCHAR);

SET @createsql = 'IF Object_id('''+@duptest+''') IS NOT NULL 
  BEGIN 
      DROP TABLE ' +@duptest+'; 
  END ;';

    SET @createsql = @createsql + 'CREATE TABLE '+@duptest+
  ' (dupcnt int,
primarykey varchar(500),
RN int
	 );';


EXEC (@createsql);


SET @PrimaryKeyPipe = 'CAST (' + REPLACE(@PrimaryKey,',',' AS VARCHAR(50))+''|''+CAST(') + ' AS VARCHAR(50))';


set @dupsql = N'INSERT INTO ' +@duptest+ ' (dupcnt,primarykey,RN)
SELECT COUNT(*), ' + @PrimaryKeyPipe + ',ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS RN FROM '  + @TableName +
' WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS VARCHAR) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS VARCHAR) +  
' AND ' + @FilterCondition + ' GROUP BY ' + @PrimaryKey + ' HAVING COUNT(*)>1;';

PRINT @dupsql;
EXEC sp_executesql @dupsql;

DECLARE @ParmDefinition0 nvarchar(500);
DECLARE @countoutsql NVARCHAR(2000);

SET @ParmDefinition0 = N'@CountOUT BIGINT OUTPUT';

SET @countoutsql = N'SELECT @CountOUT =  COUNT(1) FROM ' + @duptest+ '';


	EXECUTE sp_executesql
     @countoutsql
    ,@ParmDefinition0
    ,@CountOUT = @Cnt OUTPUT;

while @id <= @Cnt
begin

	SET @sql = '';


BEGIN
set @sql = N'INSERT INTO AUDIT_ID.[ETL_ERROR_LOG]
           ([BATCH_RUN_ID]
           ,[JOB_RUN_ID]
           ,[ERROR_REF_CD]
           ,[ERROR_RECORD_PRIMARY_KEY_VALUES]
           ,[ERROR_DTTM]
           ,[UPDATED_DTTM]
		   ,[PKG_NAME]
           ,[ERROR_COLUMNS]
           ,[ERROR_VALUE])
		SELECT '  + CAST(@BatchRunId AS VARCHAR) + ', '
          + CAST(@JobRunId AS VARCHAR) + ',
          9000004,
          primarykey,
		  GETDATE(),
          GETDATE(),
		  ''' + @SSISPackageName + ''',
          ''NA'',
	      ''' + @ErrorValue + ' present in ' + @TableName + '''
		  FROM ' + @duptest + ';'
END

EXEC sp_executesql @sql
PRINT @sql;

	
set @id = @id + 1

END
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_ErrorLogInsert]    Script Date: 8/10/2020 7:51:02 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_ErrorLogInsert'
)
   DROP PROCEDURE AUDIT_ID.USP_ErrorLogInsert
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_ErrorLogInsert] @tablename [SYSNAME],@PrimaryKeyColumnsList [nvarchar](1000),@MandatoryColumnsList [nvarchar](1000),@LBUName [nvarchar](100),@PackageName [nvarchar](100),@BatchName [nvarchar](100) AS
     
    --BEGIN TRY
     --PRINT 'AAA'
    -- Declare local variables
    DECLARE @sqlstr NVARCHAR(2000);
    DECLARE @rowcount BIGINT;
       DECLARE @RunningTotal BIGINT;
    DECLARE @PrimaryKeys nvarchar(500);
	DECLARE @JobRunId BIGINT;
	DECLARE @BatchRunId BIGINT;
DECLARE @ParmDefinition1 nvarchar(500);
DECLARE @ParmDefinition2 nvarchar(500);
DECLARE @currentstr [NVARCHAR](3000);

SET @PackageName = '''' + @PackageName + '''';
SET @BatchName = '''' + @BatchName + '''';
SET @PrimaryKeys = 'CAST (' + REPLACE(@PrimaryKeyColumnsList,',',' AS VARCHAR(50))+''|''+CAST(') + ' AS VARCHAR(50))'

     --PRINT 'AAA1'
-- Set the parameters for the sp_executesql statement
SET @ParmDefinition1 = N'@JobRunIdOut BIGINT OUTPUT';
SET @ParmDefinition2 = N'@BatchRunIdOut BIGINT OUTPUT';
SET @currentstr = N'SELECT @JobRunIdOut = MAX(JOB_RUN_ID) 
FROM AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS 
	   WHERE JOB_MASTER_ID = (
			SELECT JOB_MASTER_ID FROM [AUDIT_ID].[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = '+ @PackageName +'
			AND BATCH_MASTER_ID = (SELECT BATCH_MASTER_ID FROM [AUDIT_ID].[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = '+ @BatchName +')
		)';
	
     --PRINT 'AAA2'
	 --print @currentstr
	EXECUTE sp_executesql
     @currentstr
    ,@ParmDefinition1
    ,@JobRunIdOut = @JobRunId OUTPUT;
	
     --PRINT 'AAA3'
	SET @currentstr = N'SELECT @BatchRunIdOut = MAX(BATCH_RUN_ID) 
FROM AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS 
	   WHERE JOB_MASTER_ID = (
			SELECT JOB_MASTER_ID FROM [AUDIT_ID].[ETL_AUDIT_JOB_MASTER] WHERE SSIS_PKG_NAME = '+ @PackageName +'
			AND BATCH_MASTER_ID = (SELECT BATCH_MASTER_ID FROM [AUDIT_ID].[ETL_AUDIT_BATCH_MASTER] WHERE BATCH_NAME = '+ @BatchName +')
		)';
		
	EXECUTE sp_executesql
     @currentstr
    ,@ParmDefinition2
    ,@BatchRunIdOut = @BatchRunId OUTPUT;

	   --PRINT @JobRunId
	   --PRINT @BatchRunId
       --PRINT @PrimaryKeys
	   --print LEN(@MandatoryColumnsList)
       WHILE LEN(@MandatoryColumnsList) > 0
       BEGIN
			 print 'abc'
             DECLARE @CurrentColumnName varchar(1000);
             IF CHARINDEX(',',@MandatoryColumnsList) > 0
                    SET  @CurrentColumnName = SUBSTRING(@MandatoryColumnsList,0,CHARINDEX(',',@MandatoryColumnsList))
             ELSE
                    BEGIN
                    SET  @CurrentColumnName = @MandatoryColumnsList
                    SET @MandatoryColumnsList = ''
                    END
       
	    Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@OffsetMultipler int
		declare @LBUTime DateTime
		Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName
		select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone
		--Use Multipler to decide if we should add or subtract
		if Substring(@offset,1,1) = '+'
		begin 
			set @OffsetMultipler = 1
		end
		if Substring(@offset,1,1) = '-'
		begin
			set @OffsetMultipler = -1
		end
		set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
		set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler
		Select @LBUTime=DATEADD(Hour,@offset_Hour,GETUTCDATE())
		Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)
		--PRINT @LBUTime
		--PRINT @PrimaryKeys
		--PRINT '1'
		--print @CurrentColumnName
		--PRINT '2'
       SET @sqlstr = N'INSERT INTO [AUDIT_ID].[ETL_ERROR_LOG] ([ERROR_REF_CD],[BATCH_RUN_ID],[JOB_RUN_ID],[ERROR_RECORD_PRIMARY_KEY_VALUES],[ERROR_DTTM],[UPDATED_DTTM],[ERROR_COLUMNS],[ERROR_VALUE])
		SELECT 9000002, '+CAST(@BatchRunId as nvarchar)+', '+CAST(@JobRunId as nvarchar)+', '+@PrimaryKeys+', '''+ CONVERT(VARCHAR(20),@LBUTime) +''', '''+ CONVERT(VARCHAR(20),@LBUTime) +''', '''+@CurrentColumnName+ N''', ''NULL/BLANK Values'' 
		FROM ' + @tablename + ' WHERE (CAST(' + @CurrentColumnName + ' as varchar)'  + ' = '''' 
		OR CAST(' + @CurrentColumnName + ' as varchar) IS NULL
		) AND JOB_RUN_ID = ' + CAST(@JobRunId as nvarchar)
		--PRINT '3'
       --PRINT @sqlstr
		--PRINT '4'
       EXEC(@sqlstr)
		--PRINT '5'
       SET @MandatoryColumnsList = REPLACE(@MandatoryColumnsList,@CurrentColumnName + ',' , '')
       END 
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_KPISumCheck]    Script Date: 8/10/2020 7:51:03 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_KPISumCheck'
)
   DROP PROCEDURE AUDIT_ID.USP_KPISumCheck
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_KPISumCheck] @BatchRunId [BIGINT],@JobRunId [BIGINT],@JobMasterId [BIGINT],@LBUName [VARCHAR](100),@BalThresholdId [BIGINT],@PrimaryKey [VARCHAR](1000) AS

BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
    DECLARE @SourceTablesList varchar(2000);
    DECLARE @TgtTableName varchar(50);
    DECLARE @SrcColumnName VARCHAR(50);
    DECLARE @TgtColumnName VARCHAR(50);
	DECLARE @SourceFilterCondition VARCHAR(2000);
	DECLARE @TargetFilterCondition VARCHAR(2000);
	DECLARE @SourceAmt DECIMAL(18,5);
	DECLARE @CurrentAmt DECIMAL(18,5);
	DECLARE @TargetAmt DECIMAL(18,5);
	DECLARE @ErrorAmt DECIMAL(18,5);
	DECLARE @ErrorThreshold INT;
	DECLARE @PctOrValue VARCHAR(50);
	DECLARE @tgtstr NVARCHAR(2000);
    DECLARE @errstr NVARCHAR(2000);
    DECLARE @ParmDefinition1 nvarchar(500);
	DECLARE @ParmDefinition2 nvarchar(500);
	DECLARE @ParmDefinition3 nvarchar(500);
	DECLARE @BalRunInstanceId [INT],@BalResult [VARCHAR](100);

    -- Set the parameters for the sp_executesql statement
	SET @ParmDefinition1 = N'@TargetAmtOUT BIGINT OUTPUT';
	SET @ParmDefinition2 = N'@CurrentAmtOUT BIGINT OUTPUT';
	SET @ParmDefinition3 = N'@ErrorAmtOUT BIGINT OUTPUT';    

	/*SET @srcstr1 = N'SELECT @SourceAmtOUT1 = TOTAL_CR_AMOUNT 
	FROM ' + @SrcTableName + ' WITH (NOLOCK)';*/
	
	SELECT @SourceTablesList =  SRC_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId; 

	SELECT @SrcColumnName =  SRC_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId; 

	SELECT @TgtTableName =  TGT_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;  

	SELECT @TgtColumnName =  TGT_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

    SELECT @SourceFilterCondition =  SRC_FILTER_CONDITION FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	SELECT @TargetFilterCondition =  TGT_FILTER_CONDITION FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	print LEN(@SourceTablesList)
       WHILE LEN(@SourceTablesList) > 0
      BEGIN
             DECLARE @CurrentTableName varchar(1000);
			 DECLARE @currentstr NVARCHAR(2000);

             IF CHARINDEX(',',@SourceTablesList) > 0
                    SET  @CurrentTableName = SUBSTRING(@SourceTablesList,0,CHARINDEX(',',@SourceTablesList))
             ELSE
                    BEGIN
                    SET  @CurrentTableName = @SourceTablesList
                    SET @SourceTablesList = ''
                    END
			
	SET @currentstr = N'SELECT @CurrentAmtOUT = sum(' + @SrcColumnName + ') FROM ' + @CurrentTableName + 
	' WHERE BATCH_RUN_ID = (SELECT MAX(BATCH_RUN_ID) FROM ' + @CurrentTableName + ') AND JOB_RUN_ID = (SELECT MAX(JOB_RUN_ID) FROM ' + @CurrentTableName +') AND ' + @SourceFilterCondition;
	
	print @currentstr;
	
	EXECUTE sp_executesql
     @currentstr
    ,@ParmDefinition2
    ,@CurrentAmtOUT = @CurrentAmt OUTPUT;

	SET @SourceAmt = COALESCE(@SourceAmt,0) + COALESCE(@CurrentAmt,0);

	print @SourceAmt;

	SET @SourceTablesList = REPLACE(@SourceTablesList,@CurrentTableName + ',' , '')

	END

    SET @tgtstr = N'SELECT @TargetAmtOUT = COALESCE(SUM(' + @TgtColumnName + '),0) from ' + @TgtTableName + ' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ' AND ' + @TargetFilterCondition;

	PRINT @tgtstr;

 
	   SET @errstr = 
    N'SELECT @ErrorAmtOUT = ABS(sum(' + @TgtColumnName + ')) from ' + @TgtTableName + N' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ' AND ' + @TargetFilterCondition + N'
	AND CAST(' + @PrimaryKey + N' AS VARCHAR(1000)) IN (Select DISTINCT ERROR_RECORD_PRIMARY_KEY_VALUES FROM AUDIT_ID.[ETL_ERROR_LOG] WITH (NOLOCK) 
       WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ')';


	   PRINT @errstr;

  SELECT @ErrorThreshold = ERROR_THRESHOLD FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;     
  Select @PctOrValue=PCT_OR_VALUE FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check'      



    -- Execute the SQL String using sp_executesql, passing in the parameter definition and defining the output variable

	EXECUTE sp_executesql
    @tgtstr
    ,@ParmDefinition1
    ,@TargetAmtOUT = @TargetAmt OUTPUT;

   EXECUTE sp_executesql
     @errstr
    ,@ParmDefinition3
    ,@ErrorAmtOUT = @ErrorAmt OUTPUT;



Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4))
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3)))

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)


			 /*   IF ((CASE WHEN COALESCE(@SourceAmt,0) = 0 AND COALESCE(@TargetAmt,0) = 0
			   THEN 0
			   WHEN COALESCE(@SourceAmt,0) <> 0 AND (@TargetAmt = 0 OR @TargetAmt IS NULL)
			   THEN 100
			   WHEN COALESCE(@SourceAmt,0) = COALESCE(@TargetAmt,0) AND COALESCE(@ErrorAmt,0)=0
			   THEN 0
			   WHEN COALESCE(@SourceAmt,0) <> COALESCE(@TargetAmt,0) AND COALESCE(@ErrorAmt,0)=0
			   THEN 100
			   WHEN (COALESCE(ABS(@SourceAmt),0) >= COALESCE(ABS(@ErrorAmt),0) )
			   THEN 100 - ((CAST(COALESCE(@SourceAmt,0) - COALESCE(@ErrorAmt,0) AS FLOAT))/CAST(COALESCE(@ErrorAmt,0) AS FLOAT)) * 100
			   WHEN (COALESCE(ABS(@ErrorAmt),0) > COALESCE(ABS(@SourceAmt),0))
			   THEN 100 - ((CAST(COALESCE(@ErrorAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))/CAST(COALESCE(@ErrorAmt,0) AS FLOAT)) * 100 
			   ELSE 0 END <= COALESCE(@ErrorThreshold,0))) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'*/

			   IF ((CASE WHEN COALESCE(@SourceAmt,0) = 0 AND COALESCE(@TargetAmt,0) = 0
			   THEN 0
			   WHEN COALESCE(@SourceAmt,0) = COALESCE(@TargetAmt,0)
			   THEN 0
			   WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	           THEN CAST(((CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS FLOAT))/CAST(COALESCE(@SourceAmt,0) AS DECIMAL(16,2))) * 100 AS DECIMAL(16,2))
	           WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	           THEN CAST(((CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))/CAST(COALESCE(@TargetAmt,0) AS DECIMAL(16,2))) * 100 AS DECIMAL(16,2))
	           WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	           THEN (CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS FLOAT))
	           WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	           THEN (CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))
			   ELSE 0 END <= COALESCE(@ErrorThreshold,0))) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'

	  
	   SELECT @BalRunInstanceId=MAX(BAL_RUN_INSTANCE_ID) FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS
       BEGIN
	   /*DECLARE @maxValue int;
	   SELECT @maxValue = MAX(BAL_RUN_INSTANCE_ID) from AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
	   if @maxValue < 1 or @maxValue IS NULL
		set @maxValue = 0
	   SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] ON;*/
       Insert into AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
       (--[BAL_RUN_INSTANCE_ID],
	    [JOB_RUN_ID],
        [BAL_CHECK_ERR_THRESHOLD_ID],
        [BAL_NAME],
        [SRC_BAL_VALUE],
        [TGT_BAL_VALUE],
        [ERROR_BAL_VALUE],
        [ERROR_PCT_OR_VALUE],
        [BAL_STATUS],
        [LBU_CREATED_DTTM],
        [GMT_CREATED_DTTM])
       SELECT 
             --  @maxValue + 1,
			  @JobRunId
              ,@BalThresholdId
              ,@SrcColumnName
              ,COALESCE(@SourceAmt,0)
              ,COALESCE(@TargetAmt,0)
              ,COALESCE(@ErrorAmt,0)
              ,CASE WHEN COALESCE(@SourceAmt,0) = 0 AND COALESCE(@TargetAmt,0) = 0
			   THEN 0
			   WHEN COALESCE(@SourceAmt,0) = COALESCE(@TargetAmt,0)
			   THEN 0
			   WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	           THEN CAST(((CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS FLOAT))/CAST(COALESCE(@SourceAmt,0) AS DECIMAL(16,2))) * 100 AS DECIMAL(16,2))
	           WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	           THEN CAST(((CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))/CAST(COALESCE(@TargetAmt,0) AS DECIMAL(16,2))) * 100 AS DECIMAL(16,2))
	           WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	           THEN (CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS FLOAT))
	           WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	           THEN (CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))
			   ELSE 0 END,
			   @BalResult
              ,GetUTCdate()
              ,@LBUTime    
	   --SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] OFF;  
	   END
    
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_KPISumCheck_Stag]    Script Date: 8/10/2020 7:51:04 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_KPISumCheck_Stag'
)
   DROP PROCEDURE AUDIT_ID.USP_KPISumCheck_Stag
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_KPISumCheck_Stag] @BatchRunId [INT],@JobRunId [INT],@JobMasterId [INT],@LBUName [VARCHAR](100),@BalThresholdId [bigint],@PrimaryKey [VARCHAR](1000) AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	   DECLARE @SrcTableName VARCHAR(2000);
	   DECLARE @TgtTableName VARCHAR(50);
	   DECLARE @SrcColumnName VARCHAR(100);
	   DECLARE @TgtColumnName VARCHAR(100);
	   DECLARE @SourceFilterCondition VARCHAR(2000);
	   DECLARE @TargetFilterCondition VARCHAR(2000);
       DECLARE @SourceAmt DECIMAL(18,2);
       DECLARE @TargetAmt DECIMAL(18,2);
       DECLARE @ErrorAmt DECIMAL(18,2);
       DECLARE @ErrorThreshold INT;       
	   DECLARE @PctOrValue VARCHAR(50);
	   DECLARE @srcstr NVARCHAR(2000);
       DECLARE @tgtstr NVARCHAR(2000);
       DECLARE @errstr NVARCHAR(2000);
       DECLARE @ParmDefinition1 nvarchar(500);
       DECLARE @ParmDefinition2 nvarchar(500);
       DECLARE @ParmDefinition3 nvarchar(500);
	   DECLARE @BalRunInstanceId [INT],@BalResult [VARCHAR](100);

    -- Set the parameters for the sp_executesql statement
       SET @ParmDefinition1 = N'@SourceAmtOUT DECIMAL(18,2) OUTPUT';
       SET @ParmDefinition2 = N'@TargetAmtOUT DECIMAL(18,2) OUTPUT';
       SET @ParmDefinition3 = N'@ErrorAmtOUT DECIMAL(18,2) OUTPUT';    

	  SELECT @SrcTableName =  SRC_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @SrcColumnName =  SRC_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @TgtTableName =  TGT_TABLE_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @TgtColumnName =  TGT_COLUMN_NAME FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;
	  
	  SELECT @SourceFilterCondition =  SRC_FILTER_CONDITION FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	  SELECT @TargetFilterCondition =  TGT_FILTER_CONDITION FROM AUDIT_ID.[ETL_BALANCE_CHECK_ERROR_THRESHOLD] WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;

	   SET @srcstr =
       N'SELECT DISTINCT @SourceAmtOUT = ABS(' + @SrcColumnName + ') FROM ' + @SrcTableName + ' WITH (NOLOCK) WHERE CONTROL_FILE_ID IN (SELECT MAX(CONTROL_FILE_ID) FROM '  + @SrcTableName + ' WHERE STAG_JOB_MASTER_ID = ' + CAST(@JobMasterId AS nvarchar) + ') AND STAG_JOB_MASTER_ID = ' + CAST(@JobMasterId AS nvarchar);

	   --PRINT @TargetFilterCondition;

	   PRINT @srcstr;


	SET @tgtstr = CASE WHEN @SrcColumnName = 'TOTAL_CR_AMT'
	THEN 
       N'SELECT @TargetAmtOUT = ABS(sum(' + @TgtColumnName + ')) from ' + @TgtTableName + ' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ' AND ' + @TargetFilterCondition
	--CAST(@TgtColumnName AS nvarchar) + '< 0'
	 WHEN @SrcColumnName='TOTAL_DR_AMT' 
	 THEN 
	 N'SELECT @TargetAmtOUT = ABS(sum(' + @TgtColumnName + ')) from ' + @TgtTableName + ' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ' AND ' + @TargetFilterCondition
	--CAST(@TgtColumnName AS nvarchar) + '> 0'
	 ELSE
	 N'SELECT @TargetAmtOUT = ABS(sum(' + @TgtColumnName + ')) from ' + @TgtTableName + ' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar)
	END;

	PRINT @tgtstr;

	   SET @errstr = CASE WHEN @SrcColumnName = 'TOTAL_CR_AMT'
	THEN  
    N'SELECT @ErrorAmtOUT = ABS(sum(' + @TgtColumnName + ')) from ' + @TgtTableName + N' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ' AND ' + @TargetFilterCondition + N'
	AND CAST(' + @PrimaryKey + N' AS VARCHAR(1000)) IN (Select DISTINCT ERROR_RECORD_PRIMARY_KEY_VALUES FROM AUDIT_ID.[ETL_ERROR_LOG] WITH (NOLOCK) 
       WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ')'
	   WHEN @SrcColumnName='TOTAL_DR_AMT' 
	   THEN
       N'SELECT @ErrorAmtOUT = ABS(sum(' + @TgtColumnName + ')) from ' + @TgtTableName + N' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ' AND ' + @TargetFilterCondition + N'
	 AND CAST(' + @PrimaryKey + N' AS VARCHAR(1000)) IN (Select DISTINCT ERROR_RECORD_PRIMARY_KEY_VALUES FROM AUDIT_ID.[ETL_ERROR_LOG] WITH (NOLOCK) 
       WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ')'
	  ELSE
      N'SELECT @ErrorAmtOUT = ABS(sum(' + @TgtColumnName + ')) from ' + @TgtTableName + N' WITH (NOLOCK)
    WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) +N' 
	AND CAST(' + @PrimaryKey + N' AS VARCHAR(1000)) IN (Select DISTINCT ERROR_RECORD_PRIMARY_KEY_VALUES FROM AUDIT_ID.[ETL_ERROR_LOG] WITH (NOLOCK) 
       WHERE BATCH_RUN_ID = ' + CAST(@BatchRunId AS nvarchar) + ' AND JOB_RUN_ID = ' + CAST(@JobRunId AS nvarchar) + ')'
	   END;


	   PRINT @errstr;

  SELECT @ErrorThreshold = ERROR_THRESHOLD FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='KPI Check' AND [BAL_CHECK_ERR_THRESHOLD_ID] = @BalThresholdId;             
  Select @PctOrValue=PCT_OR_VALUE FROM AUDIT_ID.ETL_BALANCE_CHECK_ERROR_THRESHOLD WHERE JOB_MASTER_ID = @JobMasterId AND BAL_NAME='Record Count Check'      


    -- Execute the SQL String using sp_executesql, passing in the parameter definition and defining the output variable
    EXECUTE sp_executesql
     @srcstr
    ,@ParmDefinition1
    ,@SourceAmtOUT = @SourceAmt OUTPUT;
	
       EXECUTE sp_executesql
     @tgtstr
    ,@ParmDefinition2
    ,@TargetAmtOUT = @TargetAmt OUTPUT;

   EXECUTE sp_executesql
     @errstr
    ,@ParmDefinition3
    ,@ErrorAmtOUT = @ErrorAmt OUTPUT;


Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime, @OffsetMultipler INT

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

if Substring(@offset,1,1) = '+'
begin
    set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
    set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)
--PRINT @LBUTime


			   PRINT @SourceAmt
			   PRINT @TargetAmt
			   PRINT @ErrorAmt
			   PRINT @SrcColumnName


			   /*IF ((CASE WHEN COALESCE(@SourceAmt,0) = 0 AND COALESCE(@TargetAmt,0) = 0 AND (@SrcColumnName <> 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' OR @SrcColumnName <> 'TOTAL_CR_AMT + TOTAL_DR_AMT' OR COALESCE(@SrcColumnName,'0') = '0')
			   THEN 0
			   WHEN (@SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' OR @SrcColumnName ='TOTAL_CR_AMT + TOTAL_DR_AMT') AND @TargetAmt = 0
			   THEN 0
			   WHEN (@SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' OR @SrcColumnName ='TOTAL_CR_AMT + TOTAL_DR_AMT') AND @TargetAmt IS NULL
			   THEN 100
			   WHEN (@SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' OR @SrcColumnName ='TOTAL_CR_AMT + TOTAL_DR_AMT') AND @TargetAmt IS NOT NULL AND @TargetAmt <> 0
			   THEN 100
			   WHEN COALESCE(@SourceAmt,0) <> 0 AND (@TargetAmt = 0 OR @TargetAmt IS NULL)
			   THEN 100
			   WHEN COALESCE(@SourceAmt,0) = COALESCE(@TargetAmt,0) AND COALESCE(@ErrorAmt,0)=0
			   THEN 0
			   WHEN COALESCE(@SourceAmt,0) <> COALESCE(@TargetAmt,0) AND COALESCE(@ErrorAmt,0)=0
			   THEN 100
			   WHEN (COALESCE(ABS(@SourceAmt),0) >= COALESCE(ABS(@ErrorAmt),0) )
			   THEN 100 - ((CAST(COALESCE(@SourceAmt,0) - COALESCE(@ErrorAmt,0) AS FLOAT))/CAST(COALESCE(@ErrorAmt,0) AS FLOAT)) * 100
			   WHEN (COALESCE(ABS(@ErrorAmt),0) > COALESCE(ABS(@SourceAmt),0))
			   THEN 100 - ((CAST(COALESCE(@ErrorAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))/CAST(COALESCE(@ErrorAmt,0) AS FLOAT)) * 100 
			   ELSE 0 END <= COALESCE(@ErrorThreshold,0))) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'*/

	  IF ((CASE WHEN (COALESCE(@SourceAmt,0) = COALESCE(@TargetAmt,0))
	  THEN 0
	  WHEN (COALESCE(@SourceAmt,0) = 0 AND COALESCE(@TargetAmt,0) = 0 AND (@SrcColumnName <> 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' OR COALESCE(@SrcColumnName,'0') = '0'))
	  THEN 0
	  WHEN (@SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)'  AND @TargetAmt = 0)
	  THEN 0
	  /*WHEN (@PctOrValue = 'VALUE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NULL)
	  THEN COALESCE(@SourceAmt,0)
	  WHEN (@PctOrValue = 'VALUE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NOT NULL AND @TargetAmt <> 0)
	  THEN COALESCE(@TargetAmt,0)
	  WHEN (@PctOrValue = 'PERCENTAGE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NULL)
	  THEN 100
	  WHEN (@PctOrValue = 'PERCENTAGE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NOT NULL AND @TargetAmt <> 0)
	  THEN 100*/
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	  THEN CAST(((CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS FLOAT))/CAST(COALESCE(@SourceAmt,0) AS DECIMAL(16,2))) * 100 AS DECIMAL(16,2))
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	  THEN CAST(((CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))/CAST(COALESCE(@TargetAmt,0) AS DECIMAL(16,2))) * 100 AS DECIMAL(16,2))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	  THEN (CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS FLOAT))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	  THEN (CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))
	  ELSE 0 END <= COALESCE(@ErrorThreshold,0))) SET @BalResult = 'SUCCESS' ELSE SET @BalResult = 'FAILURE'

		PRINT @BalResult


	  SELECT @BalRunInstanceId=MAX(BAL_RUN_INSTANCE_ID) FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS
       BEGIN
         /* DECLARE @maxValue int;
          SELECT @maxValue = MAX(BAL_RUN_INSTANCE_ID) from AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
          if @maxValue < 1 or @maxValue IS NULL
              set @maxValue = 0
          SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] ON;*/
       Insert into AUDIT_ID.[ETL_BALANCE_RUN_DETAILS]
       (--[BAL_RUN_INSTANCE_ID],
        [JOB_RUN_ID],
        [BAL_CHECK_ERR_THRESHOLD_ID],
        [BAL_NAME],
        [SRC_BAL_VALUE],
        [TGT_BAL_VALUE],
        [ERROR_BAL_VALUE],
        [ERROR_PCT_OR_VALUE],
        [BAL_STATUS],
        [LBU_CREATED_DTTM],
        [GMT_CREATED_DTTM])
       SELECT 
              -- @maxValue+1,
              @JobRunId
              ,@BalThresholdId
              ,@SrcColumnName 
              ,COALESCE(@SourceAmt,0)
              ,COALESCE(@TargetAmt,0)
              ,COALESCE(@ErrorAmt,0)
              /*,CASE WHEN (ABS(COALESCE(@SourceAmt,0)) > ABS(COALESCE(@TargetAmt,0)) )
			   THEN CAST(ABS(((CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS DECIMAL(16,2)))/CAST(COALESCE(@SourceAmt,0) AS DECIMAL(16,2)))) * 100 AS DECIMAL(16,2))
			   WHEN (ABS(COALESCE(@TargetAmt,0)) > ABS(COALESCE(@SourceAmt,0)))
			   THEN CAST(ABS(((CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS DECIMAL(16,2)))/CAST(COALESCE(@TargetAmt,0) AS DECIMAL(16,2)))) * 100  AS DECIMAL(16,2))
			   ELSE 0 END*/
			   ,CASE WHEN (COALESCE(@SourceAmt,0) = COALESCE(@TargetAmt,0))
	  THEN 0
	  WHEN (COALESCE(@SourceAmt,0) = 0 AND COALESCE(@TargetAmt,0) = 0 AND (@SrcColumnName <> 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' OR COALESCE(@SrcColumnName,'0') = '0'))
	  THEN 0
	  WHEN (@SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)'  AND @TargetAmt = 0)
	  THEN 0
	  /*WHEN (@PctOrValue = 'VALUE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NULL)
	  THEN @SourceAmt
	  WHEN (@PctOrValue = 'VALUE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NOT NULL AND @TargetAmt <> 0)
	  THEN @TargetAmt
	  WHEN (@PctOrValue = 'PERCENTAGE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NULL)
	  THEN 100
	  WHEN (@PctOrValue = 'PERCENTAGE' AND @SrcColumnName = 'TOTAL_CR_AMT - ABS(TOTAL_DR_AMT)' AND @TargetAmt IS NOT NULL AND @TargetAmt <> 0)
	  THEN 100*/
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	  THEN CAST(((CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS FLOAT))/CAST(COALESCE(@SourceAmt,0) AS FLOAT)) * 100 AS DECIMAL(16,2))
	  WHEN (@PctOrValue = 'PERCENTAGE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	  THEN CAST(((CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS FLOAT))/CAST(COALESCE(@TargetAmt,0) AS FLOAT)) * 100 AS DECIMAL(16,2))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) > COALESCE(@TargetAmt,0))
	  THEN (CAST(COALESCE(@SourceAmt,0) - COALESCE(@TargetAmt,0) AS DECIMAL(16,2)))
	  WHEN (@PctOrValue = 'VALUE' AND COALESCE(@SourceAmt,0) < COALESCE(@TargetAmt,0))
	  THEN (CAST(COALESCE(@TargetAmt,0) - COALESCE(@SourceAmt,0) AS DECIMAL(16,2)))
	  ELSE 0 END
              ,@BalResult
              ,Getdate()
              ,@LBUTime
       --SET @BalRunInstanceId=(SELECT MAX(BAL_RUN_INSTANCE_ID) FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS)   
        --  SET IDENTITY_INSERT AUDIT_ID.[ETL_BALANCE_RUN_DETAILS] OFF;
          END 
END



GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_ProcessFailure]    Script Date: 8/10/2020 7:51:06 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_ProcessFailure'
)
   DROP PROCEDURE AUDIT_ID.USP_ProcessFailure
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_ProcessFailure] @JobMasterId [INT],@LBUName [VARCHAR](100) AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
       DECLARE @BatchRunId INT
	   DECLARE @JobRunId INT

	   SELECT @JobRunId=MAX(JOB_RUN_ID) FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE JOB_MASTER_ID = @JobMasterId
	   PRINT @JobRunId
       SELECT @BatchRunId=MAX(BATCH_RUN_ID) FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE JOB_RUN_ID = @JobRunId
       PRINT @BatchRunId

	   EXEC [AUDIT_ID].[USP_BatchFailure] @BatchRunId, @LBUName

       --If @ExecutionStatus='RUN'
       BEGIN
       
Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime,@OffsetMultipler int

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

--Use Multipler to decide if we should add or subtract
if Substring(@offset,1,1) = '+'
begin 
	set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
	set @OffsetMultipler = -1
end
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETUTCDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

       update AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS
                 SET  [EXECUTION_STATUS] = 'FAILED',
					  [GMT_END_DTTM] = GETUTCDATE(),
                      [LBU_END_DTTM] = @LBUTime
                 WHERE [JOB_RUN_ID] = @JobRunId      
       END
    
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_ProcessStart]    Script Date: 8/10/2020 7:51:06 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_ProcessStart'
)
   DROP PROCEDURE AUDIT_ID.USP_ProcessStart
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_ProcessStart] @LBUName [VARCHAR](100),@JobMasterId [INT],@BatchDate [DATE] AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
       DECLARE @BatchStartDateTime DateTime
	   DECLARE @BatchMasterId [BIGINT]
	   DECLARE @BatchRunId [BIGINT]
	   DECLARE @JobRunId [BIGINT]
	   SELECT @BatchMasterId=MAX(BATCH_MASTER_ID) FROM AUDIT_ID.ETL_AUDIT_JOB_MASTER WHERE JOB_MASTER_ID = @JobMasterId
       /* GET BatchRunId*/
	   print @BatchMasterId
       SELECT @BatchRunId=MAX(BATCH_RUN_ID) FROM AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS WHERE EXECUTION_STATUS IN ('RUN','FAILED')
	    and BATCH_MASTER_ID= @BatchMasterId
		print @BatchRunId
       BEGIN
	   IF (@BatchRunId IS NULL) 
       exec AUDIT_ID.USP_BatchStart 
                     @BatchMasterId,
                     @LBUName,
					 @BatchDate,
                     @BatchRunId OUTPUT
       END

Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime, @OffsetMultipler int

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone
--Use Multipler to decide if we should add or subtract
--print Substring(@offset,1,1)
if Substring(@offset,1,1) = '+'
begin 
	set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
	set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)
	   
      --DECLARE @JobRunId INT
       SELECT @BatchRunId=MAX(BATCH_RUN_ID) FROM AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS WHERE EXECUTION_STATUS in ('RUN','FAILED') and BATCH_MASTER_ID= @BatchMasterId
             print @BatchRunId
			 print @BatchMasterId
       IF @JobMasterId  = (Select MAX(JOB_MASTER_ID) from AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS WHERE BATCH_RUN_ID=@BatchRunId AND EXECUTION_STATUS in ('RUN','SUCCESSFUL'))
        SET @JobRunId=-1
       ELSE
       BEGIN
	   DECLARE @maxValue int
	   DECLARE @loopState int
	   set @loopState = 0
	   while @loopState = 0
	   BEGIN
       SELECT @maxValue = MAX(JOB_RUN_ID) from AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS
       if @maxValue < 1 or @maxValue IS NULL
            set @maxValue = 0
       --SET IDENTITY_INSERT AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS ON;
       INSERT INTO AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS (
			[JOB_RUN_ID]			,
			[BATCH_RUN_ID]
           ,[JOB_MASTER_ID]
           ,[LBU_START_DTTM]
           ,[LBU_END_DTTM]
           ,[GMT_START_DTTM]
           ,[GMT_END_DTTM]
           ,[EXECUTION_STATUS]
)
           SELECT @maxValue + 1,
		   @BatchRunId
           ,@JobMasterId
           ,@LBUTime 
           ,NULL
           ,Getdate()
           ,NULL
           ,'RUN'
           WHERE 0=(SELECT COUNT(*) FROM AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS WHERE JOB_RUN_ID = @maxValue + 1)
        -- SET @JobRunId = (SELECT MAX(JOB_RUN_ID) FROM AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS) 
		--SET IDENTITY_INSERT AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS OFF;
		SELECT @loopState = COALESCE(MAX(JOB_RUN_ID),0) from AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS WHERE JOB_MASTER_ID = @JobMasterId AND JOB_RUN_ID = @maxValue + 1

         END --WHILE
         END --ELSE
END
GO

/****** Object:  StoredProcedure [AUDIT_ID].[USP_ProcessSuccess]    Script Date: 8/10/2020 7:51:07 PM ******/
IF EXISTS (
  SELECT * 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'AUDIT_ID'
     AND SPECIFIC_NAME = N'USP_ProcessSuccess'
)
   DROP PROCEDURE AUDIT_ID.USP_ProcessSuccess
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [AUDIT_ID].[USP_ProcessSuccess] @JobMasterId [INT],@LBUName [VARCHAR](100) AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	--DECLARE @BatchRunId INT

	/* GET BATCH_RUN_ID*/
	--SELECT @BatchRunId=MAX(BATCH_RUN_ID) FROM AUDIT_ID.ETL_AUDIT_BATCH_RUN_DETAILS WHERE EXECUTION_STATUS='RUN'
	
	--If @ExecutionStatus='RUN'

	/* GET BatchRunId*/

	   SET NOCOUNT ON
	   DECLARE @JobRunId INT

	   SELECT @JobRunId=MAX(JOB_RUN_ID) FROM [AUDIT_ID].[ETL_AUDIT_JOB_RUN_DETAILS] WHERE JOB_MASTER_ID = @JobMasterId
	   PRINT @JobRunId
	
	BEGIN

Declare @Timezone varchar(100),@offset_Hour int,@offset_Minute int,@offset varchar(10),@LBUTime DateTime,@OffsetMultipler int,@BalResult varchar(20),@ErrorCount [INT]

Select @Timezone=TIMEZONE_NAME from AUDIT_ID.LBU_TIMEZONE where LBU_NAME=@LBUName

select @offset=current_utc_offset from sys.time_zone_info where name=@Timezone

--Use Multipler to decide if we should add or subtract
--print Substring(@offset,1,1)
if Substring(@offset,1,1) = '+'
begin 
	set @OffsetMultipler = 1
end
if Substring(@offset,1,1) = '-'
begin
	set @OffsetMultipler = -1
end
--print @OffsetMultipler
set @offset_Hour=CONVERT(INT,Substring(@offset,CHARINDEX(':',1),4)) * @OffsetMultipler
set @offset_Minute=CONVERT(INT,Reverse(Substring(Reverse(@offset),CHARINDEX(':',1),3))) * @OffsetMultipler

Select @LBUTime=DATEADD(Hour,@offset_Hour,GETDATE())

Select @LBUTime=DATEADD(MINUTE,@offset_Minute,@LBUTime)

--Select @BalResult = BAL_STATUS FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS WHERE JOB_RUN_ID = @JobRunId;
Select TOP 1 @BalResult = BAL_STATUS FROM AUDIT_ID.ETL_BALANCE_RUN_DETAILS  WHERE BAL_STATUS = 'FAILURE' AND JOB_RUN_ID = @JobRunId
Select @ErrorCount = COUNT(*) FROM [AUDIT_ID].ETL_ERROR_LOG WHERE JOB_RUN_ID = @JobRunId;

	  IF (@BalResult = 'FAILURE' OR @ErrorCount>0)
	  EXECUTE AUDIT_ID.[USP_ProcessFailure] @JobMasterId = @JobMasterId, @LBUName = @LBUName
	  
	  ELSE UPDATE AUDIT_ID.ETL_AUDIT_JOB_RUN_DETAILS 
		   SET  [EXECUTION_STATUS] = 'SUCCESSFUL',
		   [GMT_END_DTTM] = GETDATE(),
           [LBU_END_DTTM] = @LBUTime
		   WHERE [JOB_RUN_ID] = @JobRunId  
	END

END
GO


