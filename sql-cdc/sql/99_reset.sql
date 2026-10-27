-- Used by the end-to-end test only: return the POC tables to an empty state.
-- CDC must be disabled on a table before it can be dropped. Synthetic data only.

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
   AND EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'dbo_customers')
    EXEC sys.sp_cdc_disable_table
        @source_schema    = N'dbo',
        @source_name      = N'customers',
        @capture_instance = N'dbo_customers';
GO

DROP TABLE IF EXISTS dbo.customers;
DROP TABLE IF EXISTS dbo.customers_replica;
DROP TABLE IF EXISTS dbo.cdc_publisher_checkpoint;
DROP TABLE IF EXISTS dbo.cdc_applied_changes;
DROP TABLE IF EXISTS dbo.cdc_row_version;
GO
