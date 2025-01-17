CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Horn]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_horn_parts into dim_horn_parts
    MERGE [dbo].[dim_horn_parts] AS target
    USING [dbo].[staging_horn_parts] AS source
       ON (target.horn_id = source.horn_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.horn_name = source.horn_name,
            target.horn_class = source.horn_class,
            target.horn_type = source.horn_type

    WHEN NOT MATCHED THEN
        INSERT (
            horn_id,
            horn_name,
            horn_class,
            horn_type
        )
        VALUES (
            source.horn_id,
            source.horn_name,
            source.horn_class,
            source.horn_type
        );

END;
GO