using CsvToSqlETL.Models;
using CsvToSqlETL.Services.Interfaces;
using Microsoft.Extensions.Logging;

namespace CsvToSqlETL.Services.Implementations
{
    public class EtlService : IEtlService
    {
        private readonly IDataProcessor _dataProcessor;
        private readonly ICsvReader _csvReader;
        private readonly IDatabaseService _databaseService;
        private readonly ILogger<EtlService> _logger;

        /// <summary>
        /// Initializes a new instance of the EtlService class with the specified data processor, CSV reader, database service, and logger
        /// </summary>
        /// <param name="dataProcessor">Service responsible for filtering and identifying duplicates</param>
        /// <param name="csvReader">Service used to read and write CSV data</param>
        /// <param name="databaseService">Service used to interact with the database (schema creation, insert, row count)</param>
        /// <param name="logger">Logger for diagnostic and process information</param>
        public EtlService(IDataProcessor dataProcessor, ICsvReader csvReader, IDatabaseService databaseService, ILogger<EtlService> logger)
        {
            _dataProcessor = dataProcessor;
            _csvReader = csvReader;
            _databaseService = databaseService;
            _logger = logger;
        }

        /// <summary>
        /// Executes the full ETL (Extract, Transform, Load) pipeline:
        /// ensures the database schema exists, reads and processes records,
        /// writes duplicates to a CSV file, inserts valid records into the database,
        /// and returns statistics about the operation
        /// </summary>
        /// <returns>A task representing the asynchronous ETL operation, with the result containing the number of processed rows and duplicates</returns>
        public async Task<EtlResult> ProcessDataAsync()
        {
            _logger.LogInformation("Starting ETL process");
            await _databaseService.CreateSchemaIfNotExistsAsync();
            
            var (uniqueRecords, duplicates) = await _dataProcessor.ProcessRecordsAsync();
            await _csvReader.WriteDuplicatesToCsvAsync(duplicates);
            await _databaseService.BulkInsertAsync(uniqueRecords);
            var rowCount = await _databaseService.GetRowCountAsync();

            _logger.LogInformation($"ETL process completed. Total rows in database: {rowCount}");

            return new EtlResult
            {
                ProcessedRows = rowCount,
                DuplicatesCount = duplicates.Count()
            };
        }
    }
}
