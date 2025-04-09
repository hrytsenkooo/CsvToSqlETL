# CsvToSqlETL

A robust ETL (Extract, Transform, Load) application that processes taxi trip data from CSV files and loads it into a SQL Server database. This application handles data validation, deduplication, and implements efficient bulk loading strategies.

## Features

- **CSV Data Import**: efficiently reads and processes large CSV files containing taxi trip data
- **Data Validation**: validates records for logical consistency and data quality
- **Deduplication**: identifies and exports duplicate records based on specific criteria
- **Data Transformation**: converts values and ensures consistent data formatting
- **Efficient Bulk Loading**: uses SqlBulkCopy for high-performance database insertions
- **Timezone Conversion**: converts input EST datetimes to UTC when inserting into the database


## Requirements

- .NET 6.0 SDK or later
- SQL Server (local)
- Access to the input CSV file (NYC Taxi Trip data)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/hrytsenkooo/CsvToSqlETL.git
   cd CsvToSqlETL
   ```

2. Copy the example environment file and update it with your settings:
   ```
   cp .env.example .env
   ```

## Database Setup

Before running the application, make sure you have a SQL Server instance available. The application will automatically create the necessary table schema if it doesn't exist, but you need a database.

Create a new database with:

```sql
CREATE DATABASE TaxiTripsDb;
```

## Running the Application

Build and run the application:

```
dotnet build
dotnet run
```

The application will:
1. Connect to the database
2. Create the schema if it doesn't exist
3. Process the CSV data
4. Validate and transform records
5. Identify and export duplicates
6. Bulk insert valid records
7. Report the total number of processed rows and duplicates

## Query Examples

After loading the data, you can run these optimized queries:

### 1. Find PULocationId with highest average tip amount

```sql
SELECT TOP 1 
    PULocationID, 
    AVG(TipAmount) as AvgTipAmount
FROM TaxiTrips
GROUP BY PULocationID
ORDER BY AvgTipAmount DESC;
```

### 2. Find top 100 longest trips by distance

```sql
SELECT TOP 100 *
FROM TaxiTrips
ORDER BY TripDistance DESC;
```

### 3. Find top 100 longest trips by travel time

```sql
SELECT TOP 100 *,
    DATEDIFF(MINUTE, PickupDatetime, DropoffDatetime) as TravelTimeMinutes
FROM TaxiTrips
ORDER BY TravelTimeMinutes DESC;
```

### 4. Search with PULocationId condition

```sql
SELECT *
FROM TaxiTrips
WHERE PULocationID = 161
    AND TripDistance > 10;
```

### Schema Optimization

The database schema includes indices optimized for specific query patterns:

- `IX_TaxiTrips_PULocationID`: for queries filtering by pickup location
- `IX_TaxiTrips_TripDistance`: for finding top longest trips by distance
- `IX_TaxiTrips_TravelTime`: for finding top longest trips by travel time

## Handling Larger Datasets (10GB+)

To handle a 10GB CSV efficiently, I would redesign the pipeline to work in a fully streaming and memory-efficient manner-reading and validating records line-by-line without loading the entire file into memory. I’d process data in parallel using chunk-based buffering combined with Parallel.ForEach or Task.WhenAll to perform validation and transformation concurrently. For loading, I’d partition valid data into smaller batches and run multiple SqlBulkCopy operations in parallel to fully utilize database I/O throughput. Additionally, I’d consider disabling database constraints or indexes during bulk inserts and rebuilding them afterward to further speed up loading.


## Results

After processing the provided taxi trip dataset:
- Total records in the database: 293606
- Duplicates found and exported: 14
