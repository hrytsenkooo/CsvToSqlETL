using CsvToSqlETL.Config.Interface;
using CsvToSqlETL.Models;
using CsvToSqlETL.Services.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Data;

namespace CsvToSqlETL.Services.Implementations
{
    public class DatabaseService : IDatabaseService
    {
        private readonly IAppConfig _config;
        private readonly ILogger<DatabaseService> _logger;
        private const string TableName = "TaxiTrips";

        /// <summary>
        /// Initializes a new instance of the DatabaseService class with the specified configuration and logger
        /// </summary>
        /// <param name="config">Application configuration containing database connection settings</param>
        /// <param name="logger">Logger for diagnostic information</param>
        public DatabaseService(IAppConfig config, ILogger<DatabaseService> logger)
        {
            _config = config;
            _logger = logger;
        }

        /// <summary>
        /// Performs a bulk insert of trip records into the database using SqlBulkCopy
        /// for optimal performance. Converts EST datetimes to UTC before insertion.
        /// </summary>
        /// <param name="records">Collection of trip records to insert into the database</param>
        /// <returns>A task representing the asynchronous bulk insert operation</returns>
        public async Task BulkInsertAsync(IEnumerable<TripRecord> records)
        {
            if (!records.Any())
            {
                _logger.LogInformation("No records to insert");
                return;
            }

            _logger.LogInformation($"Bulk inserting {records.Count()} records");

            var dataTable = new DataTable();
            dataTable.Columns.Add("PickupDatetime", typeof(DateTime));
            dataTable.Columns.Add("DropoffDatetime", typeof(DateTime));
            dataTable.Columns.Add("PassengerCount", typeof(int));
            dataTable.Columns.Add("TripDistance", typeof(double));
            dataTable.Columns.Add("StoreAndFwdFlag", typeof(string));
            dataTable.Columns.Add("PULocationID", typeof(int));
            dataTable.Columns.Add("DOLocationID", typeof(int));
            dataTable.Columns.Add("FareAmount", typeof(decimal));
            dataTable.Columns.Add("TipAmount", typeof(decimal));

            foreach (var record in records)
            {
                dataTable.Rows.Add(
                    record.PickupDatetime.ToUniversalTime(), 
                    record.DropoffDatetime.ToUniversalTime(), 
                    record.PassengerCount,
                    record.TripDistance,
                    record.StoreAndFwdFlag,
                    record.PULocationID,
                    record.DOLocationID,
                    record.FareAmount,
                    record.TipAmount
                );
            }

            using var connection = new SqlConnection(_config.DbConnectionString);
            await connection.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = TableName,
                BatchSize = _config.BatchSize
            };

            bulkCopy.ColumnMappings.Add("PickupDatetime", "PickupDatetime");
            bulkCopy.ColumnMappings.Add("DropoffDatetime", "DropoffDatetime");
            bulkCopy.ColumnMappings.Add("PassengerCount", "PassengerCount");
            bulkCopy.ColumnMappings.Add("TripDistance", "TripDistance");
            bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "StoreAndFwdFlag");
            bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
            bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulkCopy.ColumnMappings.Add("FareAmount", "FareAmount");
            bulkCopy.ColumnMappings.Add("TipAmount", "TipAmount");

            await bulkCopy.WriteToServerAsync(dataTable);
            _logger.LogInformation("Bulk insert completed");
        }

        /// <summary>
        /// Creates the database schema if it doesn't already exist. This includes the TaxiTrips table
        /// and indexes optimized for specific query patterns:
        /// - IX_TaxiTrips_PULocationID: for queries filtering by pickup location
        /// - IX_TaxiTrips_TripDistance: for finding top longest trips by distance
        /// - IX_TaxiTrips_TravelTime: for finding top longest trips by travel time
        /// </summary>
        /// <returns>A task representing the asynchronous schema creation operation</returns>
        public async Task CreateSchemaIfNotExistsAsync()
        {
            _logger.LogInformation("Ensuring database schema exists");

            using var connection = new SqlConnection(_config.DbConnectionString);
            await connection.OpenAsync();

            var sql = @"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TaxiTrips')
                BEGIN
                    CREATE TABLE TaxiTrips (
                        Id INT IDENTITY(1,1) PRIMARY KEY,
                        PickupDatetime DATETIME2 NOT NULL,
                        DropoffDatetime DATETIME2 NOT NULL,
                        PassengerCount INT NOT NULL,
                        TripDistance FLOAT NOT NULL,
                        StoreAndFwdFlag NVARCHAR(10) NOT NULL,
                        PULocationID INT NOT NULL,
                        DOLocationID INT NOT NULL,
                        FareAmount DECIMAL(10, 2) NOT NULL,
                        TipAmount DECIMAL(10, 2) NOT NULL
                    );
                    
                    CREATE INDEX IX_TaxiTrips_PULocationID ON TaxiTrips(PULocationID);
                    CREATE INDEX IX_TaxiTrips_TripDistance ON TaxiTrips(TripDistance DESC);
                    CREATE INDEX IX_TaxiTrips_TravelTime ON TaxiTrips(DropoffDatetime, PickupDatetime);
                END
            ";

            using var command = new SqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();

            _logger.LogInformation("Database schema verified");
        }

        /// <summary>
        /// Retrieves the total number of rows in the TaxiTrips table
        /// </summary>
        /// <returns>A task representing the asynchronous operation, with the result being the row count</returns>
        public async Task<int> GetRowCountAsync()
        {
            using var connection = new SqlConnection(_config.DbConnectionString);
            await connection.OpenAsync();

            var sql = $"SELECT COUNT(*) FROM {TableName}";
            using var command = new SqlCommand(sql, connection);

            return Convert.ToInt32(await command.ExecuteScalarAsync());
        }
    }
}
