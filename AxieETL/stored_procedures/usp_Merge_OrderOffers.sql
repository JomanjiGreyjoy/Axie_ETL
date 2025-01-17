CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_OrderOffers]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_order_offers into fact_order_offers
    MERGE [dbo].[fact_order_offers] AS target
    USING [dbo].[staging_order_offers] AS source
       ON (target.order_offer_id = source.order_offer_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.axie_id = source.axie_id,
            target.added_at = source.added_at,
            target.ended_at = source.ended_at,
            target.status = source.status,
            target.payment_token = source.payment_token,
            target.base_price = source.base_price,
            target.current_price_usd = source.current_price_usd,
            target.current_price = source.current_price,
            target.ended_price = source.ended_price,
            target.bid_or_ask = source.bid_or_ask

    WHEN NOT MATCHED THEN
        INSERT (
            order_offer_id,
            axie_id,
            added_at,
            ended_at,
            status,
            payment_token,
            base_price,
            current_price_usd,
            current_price,
            ended_price,
            bid_or_ask
        )
        VALUES (
            source.order_offer_id,
            source.axie_id,
            source.added_at,
            source.ended_at,
            source.status,
            source.payment_token,
            source.base_price,
            source.current_price_usd,
            source.current_price,
            source.ended_price,
            source.bid_or_ask
        );

END;
GO
