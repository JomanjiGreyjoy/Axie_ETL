CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Eyes]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_eyes_parts into dim_eyes_parts
    MERGE [dbo].[dim_eyes_parts] AS target
    USING [dbo].[staging_eyes_parts] AS source
       ON (target.eyes_id = source.eyes_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.eyes_name = source.eyes_name,
            target.eyes_class = source.eyes_class,
            target.eyes_type = source.eyes_type

    WHEN NOT MATCHED THEN
        INSERT (
            eyes_id,
            eyes_name,
            eyes_class,
            eyes_type
        )
        VALUES (
            source.eyes_id,
            source.eyes_name,
            source.eyes_class,
            source.eyes_type
        );

END;
GO