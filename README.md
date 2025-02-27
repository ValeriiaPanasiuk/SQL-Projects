# E-commerce Data Analysis with BigQuery & Looker Studio

WITH account_data AS (
  -- Дані про акаунти
  SELECT  
    s.date AS date,
    sp.country AS country,
    a.send_interval AS send_interval,
    a.is_verified AS is_verified,
    a.is_unsubscribed AS is_unsubscribed,
    COUNT(a.id) AS account_cnt,  
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM `data-analytics-mate.DA.account` a
  JOIN `data-analytics-mate.DA.account_session` ase ON a.id = ase.account_id
  JOIN `data-analytics-mate.DA.session` s ON ase.ga_session_id = s.ga_session_id
  JOIN `data-analytics-mate.DA.session_params` sp ON s.ga_session_id = sp.ga_session_id
  GROUP BY s.date, sp.country, send_interval, is_verified, is_unsubscribed




  UNION ALL




  -- Дані про листи
  SELECT
    DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,
    sp.country AS country,
    0 AS send_interval,
    0 AS is_verified,
    0 AS is_unsubscribed,
    0 AS account_cnt,
    COUNT(es.id_message) AS sent_msg,
    COUNT(eo.id_message) AS open_msg,
    COUNT(ev.id_message) AS visit_msg
  FROM `data-analytics-mate.DA.email_sent` es
  LEFT JOIN `data-analytics-mate.DA.email_open` eo ON es.id_message = eo.id_message
  LEFT JOIN `data-analytics-mate.DA.email_visit` ev ON eo.id_message = ev.id_message
  JOIN `data-analytics-mate.DA.account_session` ase ON es.id_account = ase.account_id
  JOIN `data-analytics-mate.DA.session` s ON ase.ga_session_id = s.ga_session_id
  JOIN `data-analytics-mate.DA.session_params` sp ON s.ga_session_id = sp.ga_session_id
  GROUP BY date, country
),




aggregated_data AS (
  -- Агрегація
  SELECT *,
  SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
  SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
  FROM account_data
),




ranked_data AS (
  -- Рейтинг
  SELECT *,
  DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
 DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
  FROM aggregated_data
)




-- Фінальний вибір
SELECT *
FROM ranked_data
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10



