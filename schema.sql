-- 1. Creating a Staging Table
CREATE TABLE stg_all (
			cust_id	TEXT,
			trans_id TEXT,	
			trans_amnt TEXT,	
			amnt_sent_datetime TEXT,	
			amnt_received_datetime TEXT,	
			recipient_id TEXT,	
			trans_category TEXT,	
			payment_method TEXT,	
			trans_status TEXT,	
			cust_age TEXT,	
			sender_bank	TEXT,
			receiver_bank TEXT,	
			from_state TEXT,	
			to_state TEXT,	
			upi_app	TEXT,
			transaction_device TEXT
);


-- 2. Creating Normalized Target Tables
CREATE TABLE users (
		    user_id TEXT PRIMARY KEY,
		    cust_age INTEGER,
		    signup_channel TEXT,
		    city TEXT,
		    state TEXT
);

CREATE TABLE recipients (
		   recipient_id TEXT PRIMARY KEY,
		   category TEXT,
		   receiver_bank TEXT,
		   city TEXT,
		   state TEXT
);

CREATE TABLE transactions (
		  trans_id TEXT PRIMARY KEY,
		  trans_ts TIMESTAMP WITH TIME ZONE,
		  trans_received_ts TIMESTAMP WITH TIME ZONE,
		  payer_id TEXT,
		  payee_id TEXT,
		  amount NUMERIC(14,2),
		  trans_category TEXT,
		  payment_method TEXT,
		  status TEXT,
		  sender_bank TEXT,
		  receiver_bank TEXT,
		  from_state TEXT,
		  to_state TEXT,
		  upi_app TEXT,
		  device TEXT,
		  created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- 3. Populating Normalized Tables
-- 3.1 Populating Users Table
INSERT INTO users (user_id, cust_age, state)
SELECT DISTINCT
  NULLIF(cust_id,'') AS user_id,
  NULLIF(cust_age,'')::int AS cust_age,
  from_state AS state
FROM stg_all
WHERE NULLIF(cust_id,'') IS NOT NULL
ON CONFLICT (user_id) DO NOTHING;

-- 3.2 Populating Recipients Table
INSERT INTO recipients (recipient_id, category, receiver_bank, state)
SELECT DISTINCT
  NULLIF(recipient_id,'') AS recipient_id,
  trans_category AS category,
  receiver_bank,
  to_state AS state
FROM stg_all
WHERE NULLIF(recipient_id,'') IS NOT NULL
ON CONFLICT (recipient_id) DO NOTHING;

-- 3.3 Populating Transactions Table
INSERT INTO transactions (
  trans_id, trans_ts, trans_received_ts, payer_id, payee_id, amount,
  trans_category, payment_method, status, sender_bank, receiver_bank,
  from_state, to_state, upi_app, device
)
SELECT
  NULLIF(trans_id,'') AS trans_id,
  NULLIF(amnt_sent_datetime,'')::timestamptz AS trans_ts,
  NULLIF(amnt_received_datetime,'')::timestamptz AS trans_received_ts,
  NULLIF(cust_id,'') AS payer_id,
  NULLIF(recipient_id,'') AS payee_id,
  NULLIF(regexp_replace(trans_amnt, '[^0-9\.\-]', '', 'g'), '')::numeric(14,2) AS amount,
  trans_category,
  payment_method,
  trans_status AS status,
  sender_bank,
  receiver_bank,
  from_state,
  to_state,
  upi_app,
  transaction_device AS device
FROM stg_all
WHERE NULLIF(trans_id,'') IS NOT NULL
ON CONFLICT (trans_id) DO NOTHING;

-- 4. Creating Indexes for the Normalized Tables
CREATE INDEX idx_trans_ts ON transactions (trans_ts);
CREATE INDEX idx_transactions_payer ON transactions (payer_id);
CREATE INDEX idx_transactions_payee ON transactions (payee_id);
CREATE INDEX idx_transactions_status ON transactions (status);
CREATE INDEX idx_transactions_upi_app ON transactions (upi_app);
CREATE INDEX idx_transactions_state ON transactions (from_state, to_state);
CREATE INDEX idx_transactions_device ON transactions (device);
CREATE INDEX idx_transactions_amount_high ON transactions (trans_ts) WHERE amount > 10000;

-- 5. Exploratory Data Analysis

-- 5.1 Business Health — Volume & Top Recipients
-- A. Overall Counts and Date Range
SELECT COUNT(*) AS total_txns,
       MIN(trans_ts) AS earliest_ts,
       MAX(trans_ts) AS latest_ts
FROM transactions;

-- B. Top 20 Recipients by Gross Volume
SELECT payee_id AS recipient_id,
       COUNT(*) AS no_of_trans,
       SUM(amount) AS gross_volume,
       ROUND(AVG(amount),2) AS avg_amount
FROM transactions
GROUP BY payee_id
ORDER BY gross_volume DESC
LIMIT 20;



-- 5.2 Take Volume and Average Transactions by UPI apps
SELECT upi_app, 
	   COUNT(*) AS no_of_trans,
	   SUM(amount) AS gross_volume, 
	   ROUND(AVG(amount), 2) AS avg_trans
FROM transactions
GROUP BY upi_app
ORDER BY gross_volume DESC
LIMIT 20;



-- 5.3 Hour-of-day Activity
SELECT EXTRACT(hour FROM trans_ts) AS hour, 
	   COUNT(*) AS no_of_trans, SUM(amount) AS gross_volume
FROM transactions
GROUP BY hour
ORDER BY no_of_trans DESC;



-- 5.4 Top Origin States by Net Volume
SELECT from_state, 
	   COUNT(*) AS no_of_trans,
	   SUM(amount) AS gross_volume
FROM transactions
GROUP BY from_state
ORDER BY gross_volume DESC
LIMIT 20;



-- 5.5 Average Transaction and Transaction Frequency by Customer Age Bracket
WITH age_grp AS (
  SELECT 
    u.user_id,
    u.cust_age AS age,
    COUNT(t.trans_id) AS no_of_trans,
    AVG(t.amount) AS avg_amount
  FROM users u
  JOIN transactions t 
    ON u.user_id = t.payer_id
  WHERE u.cust_age IS NOT NULL
  GROUP BY u.user_id, u.cust_age
)
SELECT 
  CASE
    WHEN age < 25 THEN 'under 25'
    WHEN age BETWEEN 25 AND 34 THEN '25-34'
    WHEN age BETWEEN 35 AND 44 THEN '35-44'
    WHEN age BETWEEN 45 AND 54 THEN '45-54'
    ELSE '55 plus'
  END AS age_bracket,
  ROUND(AVG(no_of_trans), 2) AS avg_trans_per_user,
  ROUND(AVG(avg_amount), 2) AS avg_trans_amount
FROM age_grp
GROUP BY age_bracket
ORDER BY age_bracket;


-- 5.6 Strongest Sender-to-Recipient Connections — Top Transfer Corridors
SELECT payer_id, 
	   payee_id, 
	   COUNT(*) AS count, 
	   ROUND(SUM(amount),2) AS total_amt
FROM transactions
GROUP BY payer_id, payee_id
ORDER BY total_amt DESC
LIMIT 50;



-- 5.7 Cumulative Spend by Cohort (month-of-first-transaction)
WITH first_tx AS (
  SELECT payer_id,
         date_trunc('month', MIN(trans_ts))::date AS cohort_month
  FROM transactions
  GROUP BY payer_id
),
monthly_spend AS (
  SELECT t.payer_id,
         date_trunc('month', t.trans_ts)::date AS month,
         SUM(t.amount) AS spend
  FROM transactions t
  GROUP BY t.payer_id, date_trunc('month', t.trans_ts)
),
cohort_ltv AS (
  SELECT f.cohort_month,
         m.month,
         SUM(m.spend) AS cohort_month_spend,
         SUM(SUM(m.spend)) OVER (PARTITION BY f.cohort_month ORDER BY m.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cohort_cumulative_spend
  FROM first_tx f
  JOIN monthly_spend m ON f.payer_id = m.payer_id
  GROUP BY f.cohort_month, m.month
)
SELECT cohort_month, month, ROUND(cohort_month_spend,2) AS monthly_spend, ROUND(cohort_cumulative_spend,2) AS cumulative_spend
FROM cohort_ltv
ORDER BY cohort_month DESC, month;



-- 5.8 Daily Anomaly Detection using Rolling z-score on Daily Volume
WITH daily AS (
  SELECT date_trunc('day', trans_ts)::date AS day, SUM(amount) AS daily_volume
  FROM transactions
  GROUP BY date_trunc('day', trans_ts)
),
stats AS (
  SELECT day, daily_volume,
         AVG(daily_volume) OVER (ORDER BY day ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS roll_mean,
         STDDEV(daily_volume) OVER (ORDER BY day ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS roll_sd
  FROM daily
)
SELECT day, ROUND(daily_volume,2) AS daily_volume, ROUND(roll_mean,2) AS roll_mean, ROUND(roll_sd,2) AS roll_sd,
       CASE WHEN roll_sd IS NOT NULL AND ABS(daily_volume - roll_mean) > 3 * roll_sd THEN 'ANOMALY' ELSE 'ok' END AS flag
FROM stats
ORDER BY day DESC
LIMIT 200;






