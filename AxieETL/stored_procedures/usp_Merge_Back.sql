CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Back]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_back_parts into dim_back_parts
    MERGE [dbo].[dim_back_parts] AS target
    USING [dbo].[staging_back_parts] AS source
       ON (target.back_id = source.back_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.back_name = source.back_name,
            target.back_class = source.back_class,
            target.back_type = source.back_type

    WHEN NOT MATCHED THEN
        INSERT (
            back_id,
            back_name,
            back_class,
            back_type
        )
        VALUES (
            source.back_id,
            source.back_name,
            source.back_class,
            source.back_type
        );

END;
GO