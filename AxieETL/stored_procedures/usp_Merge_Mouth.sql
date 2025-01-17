CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Mouth]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_mouth_parts into dim_mouth_parts
    MERGE [dbo].[dim_mouth_parts] AS target
    USING [dbo].[staging_mouth] AS source
       ON (target.mouth_id = source.mouth_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.mouth_name = source.mouth_name,
            target.mouth_class = source.mouth_class,
            target.mouth_type = source.mouth_type

    WHEN NOT MATCHED THEN
        INSERT (
            mouth_id,
            mouth_name,
            mouth_class,
            mouth_type
        )
        VALUES (
            source.mouth_id,
            source.mouth_name,
            source.mouth_class,
            source.mouth_type
        );

END;
GO