-- Enable change data capture on the database and on dbo.customers. Idempotent.
-- Needs db_owner (the server admin on Azure SQL; sysadmin on SQL Server).

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE capture_instance = N'dbo_customers'
)
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'customers',
        @capture_instance     = N'dbo_customers',
        @role_name            = NULL,
        @supports_net_changes = 0;
GO
