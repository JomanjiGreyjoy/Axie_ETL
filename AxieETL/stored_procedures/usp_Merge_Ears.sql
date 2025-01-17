CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Ears]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_ears_parts into dim_ears_parts
    MERGE [dbo].[dim_ears_parts] AS target
    USING [dbo].[staging_ears_parts] AS source
       ON (target.ears_id = source.ears_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.ears_name = source.ears_name,
            target.ears_class = source.ears_class,
            target.ears_type = source.ears_type

    WHEN NOT MATCHED THEN
        INSERT (
            ears_id,
            ears_name,
            ears_class,
            ears_type
        )
        VALUES (
            source.ears_id,
            source.ears_name,
            source.ears_class,
            source.ears_type
        );

END;
GO