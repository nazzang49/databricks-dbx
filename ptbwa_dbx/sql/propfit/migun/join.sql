WITH first_join AS(
    SELECT
        dpv.*,
        CAST(gender_code AS string) AS gender,
        age_range AS age,
        carrier
    FROM      df_propfit_view dpv
    LEFT JOIN df_dmp_view ddv
    WHERE     dpv.device_id = ddv.uuid
)

SELECT fj.date,
       Cast(dayofweek(fj.date) AS string) AS dow,
       fj.device_id,
       fj.tid,
       user_pseudo_id,
       bid,
       imp,
       clk,
       gender,
       age,
       carrier,
       size,
       Cast(bidfloor AS double) AS bidfloor,
       app_category,
       Cast(device_type AS string) as device_type,
       os,
       region,
       revenue,
       itct,
       Cast(new_user AS string) AS new_user,
       session_duration,
       pageview_event,
       s0_event,
       s25_event,
       s50_event,
       s75_event,
       s100_event,
       cv,
       clk_request_miso_event,
       clk_csr_event,
       clk_request_lower_event
FROM        first_join fj
LEFT JOIN   df_ga4_view dgv
WHERE       fj.tid = dgv.tid
AND         fj.device_id = dgv.device_id