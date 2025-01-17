CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_MouthAbilities]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_mouth_abilities into dim_mouth_abilities
    MERGE [dbo].[dim_mouth_abilities] AS target
    USING [dbo].[staging_mouth_abilities] AS source
       ON (target.mouth_ability_id = source.mouth_ability_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.mouth_ability_name = source.mouth_ability_name,
            target.mouth_ability_attack = source.mouth_ability_attack,
            target.mouth_ability_defense = source.mouth_ability_defense,
            target.mouth_ability_energy = source.mouth_ability_energy,
            target.mouth_ability_description = source.mouth_ability_description

    WHEN NOT MATCHED THEN
        INSERT (
            mouth_ability_id,
            mouth_ability_name,
            mouth_ability_attack,
            mouth_ability_defense,
            mouth_ability_energy,
            mouth_ability_description
        )
        VALUES (
            source.mouth_ability_id,
            source.mouth_ability_name,
            source.mouth_ability_attack,
            source.mouth_ability_defense,
            source.mouth_ability_energy,
            source.mouth_ability_description
        );

END;
GO
