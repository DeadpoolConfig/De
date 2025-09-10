
please follow the proper 3.1 taka is instructions

You're absolutely right! Let me provide the **exact Task 3.1** implementation following the specific requirements mentioned in your assignment.

# Task 3.1: Bronze Layer - Raw Data Ingestion (Exact Requirements)

## **Objective**
Ingest raw airline data from CSV files into Spark DataFrames representing the "Bronze" (raw) layer.

## **Requirements (As Per Assignment)**
1. **Data Loading**: Load the 5 provided CSV datasets into separate Spark DataFrames
2. **Schema Inference**: Load data as-is, allowing Spark to infer the schema
3. **Temporary Views**: Register each raw DataFrame as a temporary SQL view (e.g., bronze_flights, bronze_bookings)
4. **Goal**: Capture the raw state

***

## **Bronze Layer Notebook: `bronze_layer.py`**

```python
# =================================================================
# TASK 3.1: BRONZE LAYER - RAW DATA INGESTION
# =================================================================
# Objective: Ingest raw airline data from CSV files into Spark DataFrames
# Requirements: Data Loading + Schema Inference + Temporary Views
# Architecture: Medallion Architecture - Bronze Layer (Raw Data)
# =================================================================

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TechFlight-Task3.1-Bronze-Layer") \
    .getOrCreate()

print("=== TASK 3.1: BRONZE LAYER - RAW DATA INGESTION ===")
print("Objective: Load CSV files into Spark DataFrames with schema inference")

# =================================================================
# REQUIREMENT 1: DATA LOADING
# Load the 5 provided CSV datasets into separate Spark DataFrames
# =================================================================

print("\n📂 REQUIREMENT 1: DATA LOADING")
print("Loading 5 CSV datasets into separate Spark DataFrames...")

# Update volume path with your actual volume name
VOLUME_PATH = "/Volumes/techflight_catalog/bronze_schema/your_volume_name"  # Update this path

# 1.1 Load Flights Dataset into DataFrame
print("Loading flights.csv...")
flights_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{VOLUME_PATH}/flights.csv")
print("✅ Flights DataFrame created")

# 1.2 Load Bookings Dataset into DataFrame  
print("Loading bookings.csv...")
bookings_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{VOLUME_PATH}/bookings.csv")
print("✅ Bookings DataFrame created")

# 1.3 Load Passengers Dataset into DataFrame
print("Loading passengers.csv...")
passengers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{VOLUME_PATH}/passengers.csv")
print("✅ Passengers DataFrame created")

# 1.4 Load Carriers Dataset into DataFrame
print("Loading carriers.csv...")
carriers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{VOLUME_PATH}/carriers.csv")
print("✅ Carriers DataFrame created")

# 1.5 Load Airports Dataset into DataFrame
print("Loading airports.csv...")
airports_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(f"{VOLUME_PATH}/airports.csv")
print("✅ Airports DataFrame created")

print("\n✅ ALL 5 CSV DATASETS LOADED INTO SEPARATE SPARK DATAFRAMES")

# =================================================================
# REQUIREMENT 2: SCHEMA INFERENCE
# For this layer, load the data as-is, allowing Spark to infer the schema
# Goal: Capture the raw state
# =================================================================

print("\n🔍 REQUIREMENT 2: SCHEMA INFERENCE")
print("Displaying inferred schemas (data loaded as-is):")

print("\n--- FLIGHTS DATAFRAME SCHEMA ---")
flights_df.printSchema()

print("\n--- BOOKINGS DATAFRAME SCHEMA ---")
bookings_df.printSchema()

print("\n--- PASSENGERS DATAFRAME SCHEMA ---")
passengers_df.printSchema()

print("\n--- CARRIERS DATAFRAME SCHEMA ---")
carriers_df.printSchema()

print("\n--- AIRPORTS DATAFRAME SCHEMA ---")
airports_df.printSchema()

print("\n✅ SCHEMA INFERENCE COMPLETED - RAW STATE CAPTURED")

# =================================================================
# REQUIREMENT 3: TEMPORARY VIEWS
# Register each raw DataFrame as a temporary SQL view
# (e.g., bronze_flights, bronze_bookings)
# =================================================================

print("\n📋 REQUIREMENT 3: TEMPORARY VIEWS")
print("Registering each raw DataFrame as temporary SQL view...")

# 3.1 Register Flights DataFrame as bronze_flights
flights_df.createOrReplaceTempView("bronze_flights")
print("✅ bronze_flights temporary view created")

# 3.2 Register Bookings DataFrame as bronze_bookings
bookings_df.createOrReplaceTempView("bronze_bookings")
print("✅ bronze_bookings temporary view created")

# 3.3 Register Passengers DataFrame as bronze_passengers
passengers_df.createOrReplaceTempView("bronze_passengers")
print("✅ bronze_passengers temporary view created")

# 3.4 Register Carriers DataFrame as bronze_carriers
carriers_df.createOrReplaceTempView("bronze_carriers")
print("✅ bronze_carriers temporary view created")

# 3.5 Register Airports DataFrame as bronze_airports
airports_df.createOrReplaceTempView("bronze_airports")
print("✅ bronze_airports temporary view created")

print("\n✅ ALL TEMPORARY VIEWS REGISTERED FOR EASY INSPECTION")

# =================================================================
# DELIVERABLE: DEMONSTRATION OF RAW DATA LOADING AND TEMPORARY VIEW CREATION
# =================================================================

print("\n📊 DELIVERABLE DEMONSTRATION:")
print("Raw data loading and temporary view creation completed")

# Verify temporary views are accessible using Spark SQL
print("\n1. Temporary Views Verification:")
spark.sql("SHOW VIEWS LIKE 'bronze_*'").show()

# Display row counts from each temporary view
print("\n2. Row Count Verification (using temporary views):")
spark.sql("""
    SELECT 'bronze_flights' as view_name, COUNT(*) as row_count FROM bronze_flights
    UNION ALL
    SELECT 'bronze_bookings' as view_name, COUNT(*) as row_count FROM bronze_bookings
    UNION ALL
    SELECT 'bronze_passengers' as view_name, COUNT(*) as row_count FROM bronze_passengers
    UNION ALL
    SELECT 'bronze_carriers' as view_name, COUNT(*) as row_count FROM bronze_carriers
    UNION ALL
    SELECT 'bronze_airports' as view_name, COUNT(*) as row_count FROM bronze_airports
    ORDER BY view_name
""").show()

# Display sample data to demonstrate raw state capture
print("\n3. Raw Data Sample (demonstrating unmodified state):")

print("\n--- BRONZE_FLIGHTS SAMPLE ---")
spark.sql("SELECT * FROM bronze_flights LIMIT 3").show(truncate=False)

print("\n--- BRONZE_BOOKINGS SAMPLE ---")  
spark.sql("SELECT * FROM bronze_bookings LIMIT 3").show(truncate=False)

print("\n--- BRONZE_PASSENGERS SAMPLE ---")
spark.sql("SELECT * FROM bronze_passengers LIMIT 3").show(truncate=False)

print("\n--- BRONZE_CARRIERS SAMPLE ---")
spark.sql("SELECT * FROM bronze_carriers LIMIT 3").show(truncate=False)

print("\n--- BRONZE_AIRPORTS SAMPLE ---")
spark.sql("SELECT * FROM bronze_airports LIMIT 3").show(truncate=False)

# =================================================================
# TASK 3.1 COMPLETION SUMMARY
# =================================================================

print("\n" + "="*70)
print("🎉 TASK 3.1: BRONZE LAYER COMPLETED SUCCESSFULLY")
print("="*70)
print("✅ REQUIREMENT 1: 5 CSV datasets loaded into separate Spark DataFrames")
print("✅ REQUIREMENT 2: Schema inference applied - raw state captured")
print("✅ REQUIREMENT 3: Temporary views created for easy inspection:")
print("   • bronze_flights")
print("   • bronze_bookings") 
print("   • bronze_passengers")
print("   • bronze_carriers")
print("   • bronze_airports")
print("✅ DELIVERABLE: Raw data loading and temporary view creation demonstrated")
print("✅ Ready for Task 3.2: Silver Layer (Cleaned & Conformed Data)")
print("="*70)

# =================================================================
# TASK 3.1 DELIVERABLE COMPLETE
# =================================================================
# ✅ Data Loading: 5 CSV files → Spark DataFrames ✓
# ✅ Schema Inference: Spark automatically inferred schemas ✓  
# ✅ Temporary Views: bronze_* views for easy SQL inspection ✓
# ✅ Raw State: Data captured as-is without modification ✓
# ✅ Demonstration: Notebook shows loading + view creation ✓
# =================================================================
```

***

## **Task 3.1 Requirements Checklist**

### ✅ **Data Loading**
- [x] Load flights.csv into Spark DataFrame
- [x] Load bookings.csv into Spark DataFrame  
- [x] Load passengers.csv into Spark DataFrame
- [x] Load carriers.csv into Spark DataFrame
- [x] Load airports.csv into Spark DataFrame

### ✅ **Schema Inference** 
- [x] Allow Spark to infer schema automatically
- [x] Load data as-is (raw state)
- [x] No data transformation applied

### ✅ **Temporary Views**
- [x] Register bronze_flights temporary view
- [x] Register bronze_bookings temporary view
- [x] Register bronze_passengers temporary view  
- [x] Register bronze_carriers temporary view
- [x] Register bronze_airports temporary view

### ✅ **Deliverable**
- [x] Notebook demonstrates raw data loading
- [x] Notebook demonstrates temporary view creation
- [x] Easy inspection capability provided

**This implementation follows the exact Task 3.1 requirements: Data Loading → Schema Inference → Temporary Views → Raw State Capture**

**Important**: Update the `VOLUME_PATH` variable with your actual Unity Catalog volume path where you've uploaded the CSV files.

