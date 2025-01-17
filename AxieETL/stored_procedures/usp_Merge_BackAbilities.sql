CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_BackAbilities]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_back_abilities into dim_back_abilities
    MERGE [dbo].[dim_back_abilities] AS target
    USING [dbo].[staging_back_abilities] AS source
       ON (target.back_ability_id = source.back_ability_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.back_ability_name = source.back_ability_name,
            target.back_ability_attack = source.back_ability_attack,
            target.back_ability_defense = source.back_ability_defense,
            target.back_ability_energy = source.back_ability_energy,
            target.back_ability_description = source.back_ability_description

    WHEN NOT MATCHED THEN
        INSERT (
            back_ability_id,
            back_ability_name,
            back_ability_attack,
            back_ability_defense,
            back_ability_energy,
            back_ability_description
        )
        VALUES (
            source.back_ability_id,
            source.back_ability_name,
            source.back_ability_attack,
            source.back_ability_defense,
            source.back_ability_energy,
            source.back_ability_description
        );

END;
GO
