SELECT
    Date,
    DeviceId,
    tid,
    user_pseudo_id,
    Revenue,
    ITCT,
    CASE WHEN newUser > 0 THEN '1' ELSE newUser END AS newUser,
    session_duration,
    pageview_event,
    s0_event,
    s25_event,
    s50_event,
    s75_event,
    s100_event,
    clkRequestMiso_event,
    clkCSR_event,
    requestLower_event AS clkRequestLower_event,
    CASE WHEN clkRequestMiso_event > 0 or clkCSR_event > 0 or clkRequestLower_event > 0 THEN 1.0 ELSE 0.0 END AS cv
FROM cream.propfit_ga_migun_daily
WHERE Date between '2023-03-03' and '2023-03-31'
AND Date is not null