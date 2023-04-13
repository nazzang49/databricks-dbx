SELECT
    uuid,
    gender_code,
    age_range,
    carrier
FROM ice.propfit_tg
WHERE id_type = 'ADID'