CREATE OR ALTER PROCEDURE [dbo].[usp_Merge_Axies]
AS
BEGIN
    SET NOCOUNT ON;

    -- MERGE from staging_axies into dim_axies
    MERGE [dbo].[dim_axies] AS target
    USING [dbo].[staging_axies] AS source
       ON (target.id = source.id)

    WHEN MATCHED THEN
        UPDATE SET
            target.birth_date = source.birth_date,
            target.body_shape = source.body_shape,
            target.class = source.class,
            target.color = source.color,
            target.pureness = source.pureness,
            target.purity = source.purity,
            target.stage = source.stage,
            target.hp = source.hp,
            target.morale = source.morale,
            target.skill = source.skill,
            target.speed = source.speed,
            target.eyes_id = source.eyes_id,
            target.mouth_id = source.mouth_id,
            target.ears_id = source.ears_id,
            target.horn_id = source.horn_id,
            target.back_id = source.back_id,
            target.tail_id = source.tail_id,
            target.mouth_ability_id = source.mouth_ability_id,
            target.horn_ability_id = source.horn_ability_id,
            target.back_ability_id = source.back_ability_id,
            target.tail_ability_id = source.tail_ability_id

    WHEN NOT MATCHED THEN
        INSERT (
            id,
            birth_date,
            body_shape,
            class,
            color,
            pureness,
            purity,
            stage,
            hp,
            morale,
            skill,
            speed,
            eyes_id,
            mouth_id,
            ears_id,
            horn_id,
            back_id,
            tail_id,
            mouth_ability_id,
            horn_ability_id,
            back_ability_id,
            tail_ability_id
        )
        VALUES (
            source.id,
            source.birth_date,
            source.body_shape,
            source.class,
            source.color,
            source.pureness,
            source.purity,
            source.stage,
            source.hp,
            source.morale,
            source.skill,
            source.speed,
            source.eyes_id,
            source.mouth_id,
            source.ears_id,
            source.horn_id,
            source.back_id,
            source.tail_id,
            source.mouth_ability_id,
            source.horn_ability_id,
            source.back_ability_id,
            source.tail_ability_id
        );

END;
GO
