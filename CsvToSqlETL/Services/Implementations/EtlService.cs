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

        public EtlService(IDataProcessor dataProcessor, ICsvReader csvReader, IDatabaseService databaseService, ILogger<EtlService> logger)
        {
            _dataProcessor = dataProcessor;
            _csvReader = csvReader;
            _databaseService = databaseService;
            _logger = logger;
        }

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
