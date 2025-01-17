CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Tail]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_tail_parts into dim_tail_parts
    MERGE [dbo].[dim_tail_parts] AS target
    USING [dbo].[staging_tail_parts] AS source
       ON (target.tail_id = source.tail_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.tail_name = source.tail_name,
            target.tail_class = source.tail_class,
            target.tail_type = source.tail_type

    WHEN NOT MATCHED THEN
        INSERT (
            tail_id,
            tail_name,
            tail_class,
            tail_type
        )
        VALUES (
            source.tail_id,
            source.tail_name,
            source.tail_class,
            source.tail_type
        );

END;
GO