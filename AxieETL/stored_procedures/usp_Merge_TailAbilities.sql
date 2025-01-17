CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_TailAbilities]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_tail_abilities into dim_tail_abilities
    MERGE [dbo].[dim_tail_abilities] AS target
    USING [dbo].[staging_tail_abilities] AS source
       ON (target.tail_ability_id = source.tail_ability_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.tail_ability_name = source.tail_ability_name,
            target.tail_ability_attack = source.tail_ability_attack,
            target.tail_ability_defense = source.tail_ability_defense,
            target.tail_ability_energy = source.tail_ability_energy,
            target.tail_ability_description = source.tail_ability_description

    WHEN NOT MATCHED THEN
        INSERT (
            tail_ability_id,
            tail_ability_name,
            tail_ability_attack,
            tail_ability_defense,
            tail_ability_energy,
            tail_ability_description
        )
        VALUES (
            source.tail_ability_id,
            source.tail_ability_name,
            source.tail_ability_attack,
            source.tail_ability_defense,
            source.tail_ability_energy,
            source.tail_ability_description
        );

END;
GO
