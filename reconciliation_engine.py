from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as spark_abs
import json

def run_reconciliation(config_path, base_path="data/"):
    spark = SparkSession.builder.appName("FabricReconEngine").getOrCreate()
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    # In Fabric, this would be spark.read.table(config['source']['table_path'])
    # For local assessment testing, we read the CSVs provided
    df1 = spark.read.csv(base_path + "loan_transactions.csv", header=True, inferSchema=True)
    df2 = spark.read.csv(base_path + "bank_transactions.csv", header=True, inferSchema=True)
    
    # Apply configured filters
    if config['source_1']['filter'] != "1=1":
        df1 = df1.filter(config['source_1']['filter'])
        
    join_key = config['join_keys'][0]
    amt1 = config['comparison']['source_1_amount_col']
    amt2 = config['comparison']['source_2_amount_col']
    tolerance = config['comparison']['tolerance']

    # Requirement: Handle NULL join keys[cite: 102]. 
    # Decision: Drop them before join. NULLs cannot match securely and will artificially multiply rows.
    # They are sequestered into immediate breaks.
    df1_valid = df1.filter(col(join_key).isNotNull())
    df1_nulls = df1.filter(col(join_key).isNull())
    
    df2_valid = df2.filter(col(join_key).isNotNull())
    df2_nulls = df2.filter(col(join_key).isNull())

    # Rename amount columns to avoid collision post-join
    df1_valid = df1_valid.withColumnRenamed(amt1, "loan_amt")
    df2_valid = df2_valid.withColumnRenamed(amt2, "bank_amt")

    # Full Outer Join
    joined_df = df1_valid.join(df2_valid, on=join_key, how="full_outer")

    # Categorize Outputs
    breaks_src1 = joined_df.filter(col("bank_amt").isNull()) # Missing in bank
    breaks_src2 = joined_df.filter(col("loan_amt").isNull()) # Missing in loan

    matched = joined_df.filter(col("loan_amt").isNotNull() & col("bank_amt").isNotNull())
    
    # Sensible tolerance check [cite: 101] (e.g. £0.01) to ignore micro-cent float rounding
    matched = matched.withColumn("amount_diff", spark_abs(col("loan_amt") - col("bank_amt")))
    perfect_matches = matched.filter(col("amount_diff") <= tolerance)
    mismatches = matched.filter(col("amount_diff") > tolerance)

    print(f"--- Recon Summary: {config['reconciliation_name']} ---") [cite: 103]
    print(f"Matched Transactions: {perfect_matches.count()}")
    print(f"Amount Mismatches: {mismatches.count()}")
    print(f"Breaks (Missing in Bank + NULL keys): {breaks_src1.count() + df1_nulls.count()}")
    print(f"Breaks (Missing in Loan + NULL keys): {breaks_src2.count() + df2_nulls.count()}")

if __name__ == "__main__":
    run_reconciliation("config.json")
