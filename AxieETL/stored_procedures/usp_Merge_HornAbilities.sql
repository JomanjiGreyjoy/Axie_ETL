CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_HornAbilities]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_horn_abilities into dim_horn_abilities
    MERGE [dbo].[dim_horn_abilities] AS target
    USING [dbo].[staging_horn_abilities] AS source
       ON (target.horn_ability_id = source.horn_ability_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.horn_ability_name = source.horn_ability_name,
            target.horn_ability_attack = source.horn_ability_attack,
            target.horn_ability_defense = source.horn_ability_defense,
            target.horn_ability_energy = source.horn_ability_energy,
            target.horn_ability_description = source.horn_ability_description

    WHEN NOT MATCHED THEN
        INSERT (
            horn_ability_id,
            horn_ability_name,
            horn_ability_attack,
            horn_ability_defense,
            horn_ability_energy,
            horn_ability_description
        )
        VALUES (
            source.horn_ability_id,
            source.horn_ability_name,
            source.horn_ability_attack,
            source.horn_ability_defense,
            source.horn_ability_energy,
            source.horn_ability_description
        );

END;
GO
