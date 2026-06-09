## Bronze to Silver to Gold: The Fabric Workflow

### 1. Bronze to Silver (Dataflow Gen2 & Auto-Generated Code)
*Disclaimer: The M-code provided below was NOT hand-written. It is the auto-generated backend code produced by my actions within the Dataflow Gen2 UI. I am including it to provide full visibility into the transformation logic.*

To move data from the raw Bronze layer into the structured Silver layer, I utilized the low-code capabilities of **Dataflow Gen2** rather than writing boilerplate pipeline scripts. This approach maximizes development speed and minimizes maintenance.

**The exact steps I performed in the UI for all 3 files:**
1. Connected to the raw CSV files in the OneLake Bronze layer.
2. Used the Power Query UI to visually apply data-typing (e.g., casting amounts to `Currency.Type` to prevent floating-point errors) and trimmed whitespace from key columns (`end_to_end_id`, `transaction_type`).
3. Selected my Fabric **Data Warehouse** as the exact data destination for each query.
4. Configured the destination settings to **automatically create the tables**, entirely bypassing the need to write manual DDL scripts for the staging layer.
5. Saved and ran the Dataflow Gen2 pipeline.

This process successfully structured the data and automatically generated the `silver_loan_accounts`, `silver_loan_transactions`, and `silver_bank_transactions` tables in my Warehouse. 

*(For complete transparency, the auto-generated M-code created by these UI steps is attached in the `Mahesh-Karwande/`Assessment-3 / M.code.md).*

### 2. Silver to Gold (SQL-Based Transformations)
Once the data landed securely in the Silver layer of the Warehouse, I applied **SQL-based transformations** (found in Part A of this submission) to build the Gold layer. 

By utilizing T-SQL directly within the Fabric Warehouse, I created the final, reconciled views (Perfect Matches, Mismatches, Breaks, and Account Summaries). These Gold-layer views serve as the "desired output," completely optimized and ready for the Finance team to consume directly via Power BI in Direct Lake mode.

### 3. The Configuration-Driven Engine (Part C)
While the SQL Gold layer handles the immediate reporting requirement, I built the Part C reconciliation engine using a **Fabric PySpark Notebook**. This ensures that as the organization scales and adds 20+ new reconciliations, they can be deployed entirely via JSON configuration files reading from the Silver layer, rather than requiring new SQL views to be hardcoded every time.
