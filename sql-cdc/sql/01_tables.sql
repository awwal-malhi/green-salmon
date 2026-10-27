-- Source table, target table of the same shape, and the bookkeeping tables used by
-- the publisher (checkpoint) and consumer (ledger + per-row version). Idempotent.

IF OBJECT_ID(N'dbo.customers', N'U') IS NULL
CREATE TABLE dbo.customers (
    customer_id    INT            NOT NULL CONSTRAINT pk_customers PRIMARY KEY,
    full_name      NVARCHAR(100)  NOT NULL,
    email          NVARCHAR(200)  NOT NULL,
    city           NVARCHAR(100)  NULL,
    loyalty_points INT            NOT NULL CONSTRAINT df_customers_points DEFAULT (0),
    updated_at     DATETIME2(3)   NOT NULL
);
GO

IF OBJECT_ID(N'dbo.customers_replica', N'U') IS NULL
CREATE TABLE dbo.customers_replica (
    customer_id    INT            NOT NULL CONSTRAINT pk_customers_replica PRIMARY KEY,
    full_name      NVARCHAR(100)  NOT NULL,
    email          NVARCHAR(200)  NOT NULL,
    city           NVARCHAR(100)  NULL,
    loyalty_points INT            NOT NULL CONSTRAINT df_customers_replica_points DEFAULT (0),
    updated_at     DATETIME2(3)   NOT NULL
);
GO

-- Publisher: highest CDC LSN whose changes have been sent to the queue.
IF OBJECT_ID(N'dbo.cdc_publisher_checkpoint', N'U') IS NULL
CREATE TABLE dbo.cdc_publisher_checkpoint (
    capture_instance SYSNAME     NOT NULL CONSTRAINT pk_cdc_publisher_checkpoint PRIMARY KEY,
    last_lsn         BINARY(10)  NOT NULL,
    updated_at       DATETIME2(3) NOT NULL CONSTRAINT df_cdc_publisher_checkpoint_updated DEFAULT (SYSUTCDATETIME())
);
GO

-- Consumer: one row per change applied. The primary key makes a redelivered message a no-op.
IF OBJECT_ID(N'dbo.cdc_applied_changes', N'U') IS NULL
CREATE TABLE dbo.cdc_applied_changes (
    start_lsn   BINARY(10)   NOT NULL,
    seqval      BINARY(10)   NOT NULL,
    operation   TINYINT      NOT NULL,
    customer_id INT          NOT NULL,
    outcome     VARCHAR(10)  NOT NULL,  -- 'applied' or 'stale' (older than what the row already reflects)
    applied_at  DATETIME2(3) NOT NULL CONSTRAINT df_cdc_applied_changes_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT pk_cdc_applied_changes PRIMARY KEY (start_lsn, seqval, operation, customer_id)
);
GO

-- Consumer: the CDC position each target row reflects, so out-of-order delivery cannot
-- overwrite a newer state (kept after deletes as a tombstone).
IF OBJECT_ID(N'dbo.cdc_row_version', N'U') IS NULL
CREATE TABLE dbo.cdc_row_version (
    customer_id INT        NOT NULL CONSTRAINT pk_cdc_row_version PRIMARY KEY,
    start_lsn   BINARY(10) NOT NULL,
    seqval      BINARY(10) NOT NULL,
    is_deleted  BIT        NOT NULL
);
GO
