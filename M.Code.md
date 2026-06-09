#Section: Bronze to Silver Transformations (Dataflow Gen2)
##Strategic Context
As part of the Fabric Medallion architecture, the raw CSV files landed in the Bronze layer (OneLake Files). Instead of writing boilerplate Python code for basic data typing and trimming, I utilized Dataflow Gen2 to visually construct the ETL pipeline, standardizing the data before publishing it as Delta Parquet tables into the Silver layer.

Below are the Power Query steps and the auto-generated M-code for each data stream. The core transformations focused on:

Promoting headers and resolving encoding.

Strict Type Casting: Ensuring dates are parsed correctly and monetary amounts are cast to Currency.Type (Decimal(15,2) equivalent) to prevent floating-point errors downstream.

String Sanitization: Trimming whitespace from critical join keys (end_to_end_id) and categorical columns (transaction_type) to prevent false-positive breaks during reconciliation.

1. Silver Loan Accounts (silver_loan_accounts)
Power Query Applied Steps:

Connect to OneLake (Bronze Files).

Parse CSV Document.

Promote Headers.

Change Column Types (Dates to type date, Amounts to Currency.Type).

M-Code:

let
    // 1. Connect to Bronze OneLake 
    Source = Lakehouse.Contents(null){[workspaceId="<workspace_id>"]}[Data]{[lakehouseId="<lakehouse_id>"]}[Data],
    #"Navigation 1" = Source{[Id="Files",ItemKind="Folder"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Name="loan_accounts.csv"]}[Content],
    
    // 2. Parse CSV
    #"Imported CSV" = Csv.Document(#"Navigation 2",[Delimiter=",", Columns=11, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars=true]),
    
    // 3. Strict Type Casting
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{
        {"account_id", type text}, 
        {"customer_id", type text}, 
        {"product_code", type text}, 
        {"product_name", type text}, 
        {"status", type text}, 
        {"opened_date", type date}, 
        {"maturity_date", type date}, 
        {"principal_amount", Currency.Type}, 
        {"outstanding_balance", Currency.Type}, 
        {"interest_rate_pct", type number}, 
        {"self_build_flag", type text}
    })
in
    #"Changed Type"

##2. Silver Loan Transactions (silver_loan_transactions)
*Power Query Applied Steps:

Connect to OneLake (Bronze Files).

Parse CSV Document & Promote Headers.

Change Column Types (Amounts to Currency.Type).

Sanitize Strings: Trim whitespace from transaction_type and end_to_end_id to ensure clean PySpark joins later.

**M-Code:

let
    // 1. Connect to Bronze OneLake
    Source = Lakehouse.Contents(null){[workspaceId="<workspace_id>"]}[Data]{[lakehouseId="<lakehouse_id>"]}[Data],
    #"Navigation 1" = Source{[Id="Files",ItemKind="Folder"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Name="loan_transactions.csv"]}[Content],
    
    // 2. Parse CSV
    #"Imported CSV" = Csv.Document(#"Navigation 2",[Delimiter=",", Columns=8, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars=true]),
    
    // 3. Strict Type Casting
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{
        {"transaction_id", type text}, 
        {"account_id", type text}, 
        {"transaction_type", type text}, 
        {"amount", Currency.Type}, 
        {"transaction_date", type date}, 
        {"value_date", type date}, 
        {"end_to_end_id", type text}, 
        {"narrative", type text}
    }),
    
    // 4. Data Cleansing (Crucial for Recon Joins)
    #"Trimmed Text" = Table.TransformColumns(#"Changed Type",{
        {"transaction_type", Text.Trim, type text}, 
        {"end_to_end_id", Text.Trim, type text}
    })
in
    #"Trimmed Text"

##3. Silver Bank Transactions (silver_bank_transactions)
#Power Query Applied Steps:

Connect to OneLake (Bronze Files).

Parse CSV Document & Promote Headers.

Change Column Types (transaction_time to type datetime, Amounts to Currency.Type).

Sanitize Strings: Trim whitespace from the end_to_end_id.

M-Code:

let
    // 1. Connect to Bronze OneLake
    Source = Lakehouse.Contents(null){[workspaceId="<workspace_id>"]}[Data]{[lakehouseId="<lakehouse_id>"]}[Data],
    #"Navigation 1" = Source{[Id="Files",ItemKind="Folder"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Name="bank_transactions.csv"]}[Content],
    
    // 2. Parse CSV
    #"Imported CSV" = Csv.Document(#"Navigation 2",[Delimiter=",", Columns=8, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars=true]),
    
    // 3. Strict Type Casting
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{
        {"bank_transaction_id", type text}, 
        {"end_to_end_id", type text}, 
        {"account_number", type text}, 
        {"counterparty_account", type text}, 
        {"debit_credit", type text}, 
        {"amount", Currency.Type}, 
        {"transaction_time", type datetime}, 
        {"narrative", type text}
    }),
    
    // 4. Data Cleansing (Crucial for Recon Joins)
    #"Trimmed Text" = Table.TransformColumns(#"Changed Type",{
        {"end_to_end_id", Text.Trim, type text}
    })
in
    #"Trimmed Text"



#Because Data Engineering is about efficiency. Standardizing data types, handling schema evolution, and trimming white spaces are highly repetitive ETL tasks. Dataflow Gen2 handles these instantly with no code maintenance. Python/PySpark is expensive and powerful compute—I saved the PySpark Notebook exclusively for the complex, configuration-driven business logic (the reconciliation engine itself) rather than wasting compute time on basic string manipulation.
