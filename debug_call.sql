USE [Enron_2018]
GO

DECLARE	@return_value Int

EXEC	@return_value = [tSQLt].[ResultSetFillTable]
		@tableString = N'file_management_job_progress_get_unit.actual,file_management_job_progress_get_unit.actual_reaction',
		@Command = N'exec dbo.file_management_job_progress_get @rpf_jobID = 7370'

SELECT	@return_value as 'Return Value'
--DECLARE	@return_value Int

--EXEC	@return_value = [tSQLt].[ResultSetFilter]
--		@resultSetNo = 2,
--		@Command = N'exec dbo.file_management_job_progress_get @rpf_jobID = 7370'

--SELECT	@return_value as 'Return Value'
GO
