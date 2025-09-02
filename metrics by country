WITH
-- Email metrics: sent, open, visit counts by date, country and account settings
  email_metrics AS (
  SELECT
    DATE_ADD(ss.date, INTERVAL es.sent_date DAY) AS date,
    sp.country,
    ac.send_interval,
    ac.is_verified,
    ac.is_unsubscribed,
    COUNT(DISTINCT es.id_message) AS sent_msg,
    COUNT(DISTINCT eo.id_message) AS open_msg,
    COUNT(DISTINCT ev.id_message) AS visit_msg
  FROM
    `DA.email_sent` es
  LEFT JOIN
    `DA.email_open` eo
  ON
    es.id_message = eo.id_message
  LEFT JOIN
    `DA.email_visit` ev
  ON
    es.id_message = ev.id_message
  JOIN
    `DA.account_session` acs
  ON
    es.id_account = acs.account_id
  JOIN
    `DA.session` ss
  ON
    acs.ga_session_id = ss.ga_session_id
  JOIN
    `DA.session_params` sp
  ON
    acs.ga_session_id = sp.ga_session_id
  JOIN
    `DA.account` ac
  ON
    acs.account_id = ac.id
  GROUP BY
    1,
    2,
    3,
    4,
    5 ),
-- Account metrics: number of created accounts per day, segmented by country and account settings
  account_metrics AS (
  SELECT
    ss.date AS date,
    sp.country,
    ac.send_interval AS send_interval,
    ac.is_verified AS is_verified,
    ac.is_unsubscribed AS is_unsubscribed,
    COUNT(acs.account_id) AS account_cnt
  FROM
    `DA.account` ac
  JOIN
    `DA.account_session` acs
  ON
    ac.id = acs.account_id
  JOIN
    `DA.session` ss
  ON
    acs.ga_session_id = ss.ga_session_id
  JOIN
    `DA.session_params` sp
  ON
    acs.ga_session_id = sp.ga_session_id
  GROUP BY
    1,
    2,
    3,
    4,
    5 ),
-- Combine account and email metrics
  combined AS (
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    NULL AS sent_msg,
    NULL AS open_msg,
    NULL AS visit_msg
  FROM
    account_metrics
  UNION ALL
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    NULL AS account_cnt,
    sent_msg,
    open_msg,
    visit_msg
  FROM
    email_metrics ),
-- Aggregate all metrics per combination of date, country and account parameters
  aggregated AS(
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg,
    SUM(SUM(account_cnt)) OVER(PARTITION BY country) AS total_country_account_cnt,
    SUM(SUM(sent_msg)) OVER(PARTITION BY country) AS total_country_sent_cnt
  FROM
    combined
  GROUP BY
    1,
    2,
    3,
    4,
    5 ),
 -- country ranks by total account count and sent email count
  ranked AS(
  SELECT
    *,
    DENSE_RANK() OVER(ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
    DENSE_RANK() OVER(ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
  FROM
    aggregated )

SELECT
  *
FROM
  ranked
WHERE
  rank_total_country_account_cnt <= 10
  OR rank_total_country_sent_cnt <= 10
ORDER BY
  date,
  country;
