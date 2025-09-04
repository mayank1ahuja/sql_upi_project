![Project Header]

<h1 align = 'center' > 🤳🏻₹ Merchant Economics for Digital Payments: A Relational Analytics Approach 🤳🏻₹ </h1>


## **Project Overview**
This repository is a production-minded, PostgreSQL-first analytics pipeline for transaction-level wallet / UPI data. It converts raw transaction records into a normalized relational model and delivers product-grade answers on revenue leakage, merchant profitability, geographic adoption, retention dynamics, and fraud/AML signals. The deliverable is intentionally resume-ready: clear problem framing, repeatable SQL analyses, and reproducible visualizations.


## **Problem Statement**
Optimize merchant economics, user monetization and operational risk for a modern digital-wallet platform. Ingest transaction-level wallet/UPI data, stage messy inputs, normalize into `users`, `recipients` (merchants/recipients), and `transactions`, then use SQL to answer operationally critical questions:

- Where is revenue leaking (fees vs refunds)?  
- Which merchants drive the most profitable volume?  
- Which geographies show the fastest adoption?  
- How does transaction velocity map to retention?  
- Which transaction patterns indicate fraud or AML risk?

This project demonstrates payments product thinking, ops optimisation, and data-driven policy recommendations.


## **Repo Layout**
```
├── schema.sql                    
├── UPI Transactions 2023-24.csv    
├── visualizations.ipynb          
└── README.md                     
```


## **Project Goals**
1. Demonstrate a repeatable SQL-first pipeline for payments analytics (ingest → stage → normalize → analyze).  
2. Surface product and ops insights: leakage, merchant profitability, geo adoption, retention, and fraud signals.  
3. Produce clean, recruiter-facing artifacts: normalized schema, materialized queries, and polished visualizations.


## **Dataset**
- *Source:* [Kaggle]
- *Dataset:* 


## **Key Design Choices**
- **Staging-first ingestion (`stg_all`)** to keep raw inputs immutable and isolate parsing logic.  
- **Normalized target model**: `users`, `recipients`, `transactions` for concise analytics joins and referential integrity.  
- **Indexing strategy**: indexes on timestamps, payer/payee, status, UPI app, state, device, and conditional index for large-value transactions.  
- **SQL-native analytics**: cohort LTV, corridor analysis, rolling z-score anomaly detection, and merchant ranking implemented as repeatable SQL blocks.  
- **Notebook for visual storytelling**: `visualizations.ipynb` demonstrates EDA-driven visuals using Pandas + Matplotlib/Seaborn and exports charts for recruiter-friendly presentation.

## **Workflow**

### **0. Requirements**
- Python 3.9+ (Pandas, Numpy, Plotly)
- PostgreSQL 12+  
- Jupyter Notebook

### **1. Create the Database & Schema**
```bash
psql -U <user> -d <db_name> -f schema.sql
```

### **2. Create Tables**
```sql
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
```

### 3. **Populate Normalized Tables**
```sql
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
```

### **4. Running thw Core Analysis**
```sql
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
```
### **5. Visualisation of SQL Queries**
```python
# Query: Top 20 Recipients by Gross Volume
query = """
SELECT payee_id AS recipient_id,
       COUNT(*) AS no_of_trans,
       SUM(amount) AS gross_volume
FROM transactions
GROUP BY payee_id
ORDER BY gross_volume DESC
LIMIT 20;
"""
df = pd.read_sql(query, conn)

fig = px.bar(
    df.sort_values('gross_volume', ascending = True),
    x = 'gross_volume', y = 'recipient_id',
    orientation = 'h',
    text = 'gross_volume',
    color = 'gross_volume',
    color_continuous_scale = px.colors.sequential.Turbo,
    labels = {'gross_volume':'Gross volume','recipient_id':'Recipient'}
)
fig.update_traces(texttemplate = '%{text:.2f}', textposition = 'outside')
fig.update_layout(title = "Top 20 Recipients by Gross Volume", margin = dict(l = 200))
```

```python
# Query: Take Volume and Average Transactions by UPI apps
query = """
SELECT upi_app,
       COUNT(*) AS no_of_trans,
       SUM(amount) AS gross_volume,
       ROUND(AVG(amount),2) AS avg_trans
FROM transactions
GROUP BY upi_app
ORDER BY gross_volume DESC
LIMIT 30;
"""
df = pd.read_sql(query, conn)

fig = px.treemap(df, path = ['upi_app'], values = 'gross_volume',
                 color = 'avg_trans', color_continuous_scale = px.colors.sequential.Plasma,
                 hover_data = {'no_of_trans':True, 'avg_trans':True})
fig.update_layout(title = 'UPI Apps — Share of Volume(Colored by Transactions)')
```

```python
# Query: Hour-of-day Activity
query = """
SELECT EXTRACT(hour FROM trans_ts) AS hour,
       COUNT(*) AS no_of_trans,
       SUM(amount) AS gross_volume
FROM transactions
GROUP BY hour
ORDER BY hour;
"""
df = pd.read_sql(query, conn)

fig = go.Figure()
fig.add_trace(go.Scatter(
    x = df['hour'], y=df['no_of_trans'],
    mode = 'lines+markers', line = dict(shape = 'spline', width = 3),
    marker=dict(size=6),
))
fig.update_layout(title = 'Transactions by Hour of Day', xaxis_title = 'Hour (0-23)', yaxis_title = 'Count')
```

```python
# Query: Daily Anomaly Detection using Rolling z-score on Daily Volume
query = """
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
"""
df = pd.read_sql(query, conn, parse_dates = ['day'])

# plot
d = df.sort_values('day')
fig = go.Figure()
fig.add_trace(go.Scatter(x = d['day'], y = d['daily_volume'], mode = 'lines', name = 'Daily volume', line = dict(width = 2, color = 'green')))
fig.add_trace(go.Scatter(x = d['day'], y = d['roll_mean'], mode = 'lines', name = 'Rolling mean', line = dict(width = 2, dash = 'dash', color = 'red')))
anoms = d[d['flag'] == 'ANOMALY']
fig.add_trace(go.Scatter( x = anoms['day'], y = anoms['daily_volume'], mode = 'markers', marker = dict(size = 10, color = 'red', symbol = 'x'), name = 'Anomalies'))
fig.update_layout(title = 'Daily Volume with Anomalies', xaxis_title = 'Day', yaxis_title = 'Daily volume')
```

```python
# Query: Average Transaction and Transaction Frequency by Customer Age Bracket
query = """
WITH age_grp AS (
  SELECT u.user_id, u.cust_age AS age,
         COUNT(t.trans_id) AS no_of_trans,
         AVG(t.amount) AS avg_amount
  FROM users u
  JOIN transactions t ON u.user_id = t.payer_id
  WHERE u.cust_age IS NOT NULL
  GROUP BY u.user_id, u.cust_age
)
SELECT CASE
         WHEN age < 25 THEN 'under 25'
         WHEN age BETWEEN 25 AND 34 THEN '25-34'
         WHEN age BETWEEN 35 AND 44 THEN '35-44'
         WHEN age BETWEEN 45 AND 54 THEN '45-54'
         ELSE '55 plus' END AS age_bracket,
       ROUND(AVG(no_of_trans),2) AS avg_trans_per_user,
       ROUND(AVG(avg_amount),2) AS avg_trans_amount
FROM age_grp
GROUP BY age_bracket
ORDER BY age_bracket;
"""
df = pd.read_sql(query, conn)

fig1 = px.bar(df, x = 'age_bracket', y = 'avg_trans_per_user', title = 'Average Transactions per User by Age Bracket',
              color = 'age_bracket', color_continuous_scale = px.colors.sequential.Magma)
fig2 = px.bar(df, x = 'age_bracket', y = 'avg_trans_amount', title = 'Average Transaction Amount by Age Bracket',
              color = 'avg_trans_amount', color_continuous_scale = px.colors.sequential.Viridis)
fig1.show()
fig2.show()
```

## **Future Scope**
- Adding a dedicated `refunds` table and materialized reconciliation views to compute net revenue per merchant.  
- Building a lightweight scorecard (materialized views + triggers) to track leakage & fraud signals automatically.  

## Reproducibility checklist
- [ ] PostgreSQL database created and `schema.sql` applied.  
- [ ] `stg_all` loaded with the provided CSV.  
- [ ] Normalized tables populated via `INSERT SELECT` blocks.  
- [ ] Notebook run end-to-end to re-generate visual assets.


## **Authon -** *Mayank Ahuja*
