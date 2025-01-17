CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Transfers]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_transfers into fact_transfers
    MERGE [dbo].[fact_transfers] AS target
    USING [dbo].[staging_transfers] AS source
       ON (target.transfer_id = source.transfer_id AND target.axie_id = source.axie_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.timestamp      = source.timestamp,
            target.with_price_usd = source.with_price_usd,
            target.with_price     = source.with_price

    WHEN NOT MATCHED THEN
        INSERT (
            transfer_id,
            axie_id,
            timestamp,
            with_price_usd,
            with_price
        )
        VALUES (
            source.transfer_id,
            source.axie_id,
            source.timestamp,
            source.with_price_usd,
            source.with_price
        );

END;
GO