# Step 1: Setting up the Fabric Workspace (The Prep)

set up a Medallion Architecture using a single Fabric Workspace:

* Bronze (Raw): I created a Fabric Lakehouse and uploaded the three provided CSVs (loan_accounts.csv, loan_transactions.csv, bank_transactions.csv) into the Files/ section.

* Silver (Structured): I used a Dataflow Gen2 to clean these CSVs, enforce data types, and publish them as Delta Tables in the Lakehouse.

* Gold (Serving): The reconciled data sits in a Fabric Data Warehouse or Lakehouse SQL Analytics Endpoint, ready for Power BI.

Part A — SQL Reconciliation (T-SQL)
I have executed these queries using the SQL Analytics Endpoint of my Fabric Lakehouse or inside a Fabric Data Warehouse.

# A1. DDL (Table Definitions)
## Note: If we use Dataflow Gen2 to load the tables, Fabric creates these automatically.

CREATE TABLE dbo.loan_accounts (
    account_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_code VARCHAR(50),
    product_name VARCHAR(100),
    status VARCHAR(20),
    opened_date DATE,
    maturity_date DATE,
    principal_amount DECIMAL(15,2),
    outstanding_balance DECIMAL(15,2),
    interest_rate_pct DECIMAL(5,2),
    self_build_flag CHAR(1)
);

CREATE TABLE dbo.loan_transactions (
    transaction_id VARCHAR(50),
    account_id VARCHAR(50),
    transaction_type VARCHAR(50),
    amount DECIMAL(15,2),
    transaction_date DATE,
    value_date DATE,
    end_to_end_id VARCHAR(100),
    narrative VARCHAR(255)
);

CREATE TABLE dbo.bank_transactions (
    bank_transaction_id VARCHAR(50),
    end_to_end_id VARCHAR(100),
    account_number VARCHAR(50),
    counterparty_account VARCHAR(50),
    debit_credit VARCHAR(10),
    amount DECIMAL(15,2),
    transaction_time DATETIME2,
    narrative VARCHAR(255)
);

## Query 1 — Matched transactions

SELECT 
    lt.transaction_id, bt.bank_transaction_id, lt.end_to_end_id,
    lt.amount AS loan_amount, bt.amount AS bank_amount
FROM dbo.loan_transactions lt
INNER JOIN dbo.bank_transactions bt ON lt.end_to_end_id = bt.end_to_end_id
WHERE lt.transaction_type = 'REPAYMENT' AND lt.amount = bt.amount;


## Query 2 — Amount mismatches


SELECT 
    lt.transaction_id, bt.bank_transaction_id, lt.end_to_end_id,
    lt.amount AS loan_amount, bt.amount AS bank_amount,
    ABS(lt.amount - bt.amount) AS amount_difference
FROM dbo.loan_transactions lt
INNER JOIN dbo.bank_transactions bt ON lt.end_to_end_id = bt.end_to_end_id
WHERE lt.transaction_type = 'REPAYMENT' AND lt.amount <> bt.amount
ORDER BY amount_difference DESC;


## Query 3 — Breaks


SELECT 
    COALESCE(lt.end_to_end_id, bt.end_to_end_id) AS end_to_end_id,
    lt.transaction_id AS loan_transaction_id,
    bt.bank_transaction_id,
    lt.amount AS loan_amount, bt.amount AS bank_amount,
    CASE 
        WHEN bt.bank_transaction_id IS NULL THEN 'Missing in Bank'
        WHEN lt.transaction_id IS NULL THEN 'Missing in Loan System'
    END AS break_type
FROM (SELECT * FROM dbo.loan_transactions WHERE transaction_type = 'REPAYMENT') lt
FULL OUTER JOIN dbo.bank_transactions bt ON lt.end_to_end_id = bt.end_to_end_id
WHERE lt.transaction_id IS NULL OR bt.bank_transaction_id IS NULL;


## Query 4 — Duplicates


-- Loan System Duplicates
SELECT end_to_end_id, amount, transaction_date, COUNT(*) as duplicate_count
FROM dbo.loan_transactions
WHERE end_to_end_id IS NOT NULL AND transaction_type = 'REPAYMENT'
GROUP BY end_to_end_id, amount, transaction_date HAVING COUNT(*) > 1;

-- Bank System Duplicates
SELECT end_to_end_id, amount, CAST(transaction_time AS DATE) AS transaction_date, COUNT(*) as duplicate_count
FROM dbo.bank_transactions
WHERE end_to_end_id IS NOT NULL
GROUP BY end_to_end_id, amount, CAST(transaction_time AS DATE) HAVING COUNT(*) > 1;


## Query 5 — Per-account summary


WITH Expected AS (
    SELECT account_id, SUM(amount) AS expected_repayments
    FROM dbo.loan_transactions WHERE transaction_type = 'REPAYMENT' GROUP BY account_id
),
Actual AS (
    SELECT account_number, SUM(amount) AS actual_receipts
    FROM dbo.bank_transactions GROUP BY account_number
)
SELECT 
    la.account_id,
    ISNULL(e.expected_repayments, 0) AS expected,
    ISNULL(a.actual_receipts, 0) AS actual,
    ISNULL(a.actual_receipts, 0) - ISNULL(e.expected_repayments, 0) AS variance,
    CASE 
        WHEN ISNULL(a.actual_receipts, 0) = ISNULL(e.expected_repayments, 0) THEN 'RECONCILED'
        WHEN ISNULL(a.actual_receipts, 0) > ISNULL(e.expected_repayments, 0) THEN 'UNALLOCATED CASH'
        ELSE 'SHORTFALL'
    END AS status
FROM dbo.loan_accounts la
LEFT JOIN Expected e ON la.account_id = e.account_id
LEFT JOIN Actual a ON la.account_id = a.account_number;




# Part B — Pipeline & Architecture Design (The Fabric Way)

B1. Architecture Overview

Ingestion: A Fabric Data Pipeline uses an On-Premises Data Gateway or Managed VNet to extract data from the legacy cloud DW into OneLake (Bronze layer).

Transformation: Dataflow Gen2 applies visual data-typing, cleansing, and writes Delta tables to the Lakehouse (Silver layer).

Reconciliation Engine: A Fabric Notebook executes the configuration-driven matching logic and writes results to the Fabric Data Warehouse (Gold layer).

Consumption: Finance connects to the Gold layer using Power BI Direct Lake mode (zero data duplication, real-time speed).

Alerting: Data Activator (Reflex) monitors the Power BI semantic model natively. If breaks > 10 or value > £1000, it triggers a Teams alert and an Office 365 email.

B2. Design Answers

Safe Reprocessing: Fabric pipelines are idempotent. Using a @pipeline().parameters.TargetDate parameter, we can re-run yesterday by overwriting that specific date partition in OneLake. If we need 30 days, we wrap the pipeline in a ForEach loop.

Alerting Strategy: Pipeline failures (e.g., source disconnected) are handled by Data Pipeline failure paths. Data anomalies (e.g., £1,000 breaks) are handled entirely by Data Activator. They are distinct alerts serving distinct personas (Engineers vs. Finance).

Why Fabric? (Cost & Reuse): Traditional Azure architectures require stitching ADF, Databricks, SQL, Key Vault, and Logic Apps together. By consolidating into Microsoft Fabric, we eliminate cross-service network egress costs, reduce security perimeter complexity (OneSecurity), and allow the team to manage configuration, compute, and BI in a single workspace.

## Part C — Implementation Task (The Engine)
Crucial Context: The brief strictly demands that adding a new reconciliation must be a "configuration change, not a code change." Dataflow Gen2 cannot do this dynamically from a JSON file. To stay native to Fabric while satisfying this hard rule, we use a Fabric Spark Notebook. It sits perfectly in the Fabric workspace and runs serverlessly.
