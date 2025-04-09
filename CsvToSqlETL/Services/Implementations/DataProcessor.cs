using CsvToSqlETL.Models;
using CsvToSqlETL.Services.Interfaces;
using Microsoft.Extensions.Logging;

namespace CsvToSqlETL.Services.Implementations
{
    public class DataProcessor : IDataProcessor
    {
        private readonly ICsvReader _csvReader;
        private readonly ILogger<DataProcessor> _logger;

        public DataProcessor(ICsvReader csvReader, ILogger<DataProcessor> logger)
        {
            _csvReader = csvReader;
            _logger = logger;
        }

        public async Task<(IEnumerable<TripRecord> UniqueRecords, IEnumerable<TripRecord> Duplicates)> ProcessRecordsAsync()
        {
            _logger.LogInformation("Processing records");

            var uniqueRecords = new Dictionary<(DateTime, DateTime, int), TripRecord>();
            var duplicates = new List<TripRecord>();
            var recordCount = 0;

            await foreach (var record in _csvReader.ReadRecordAsync())
            {
                recordCount++;

                if (recordCount % 1000 == 0)
                {
                    _logger.LogInformation($"Processed {recordCount} records so far");
                }

                var key = (record.PickupDatetime, record.DropoffDatetime, record.PassengerCount);

                if (uniqueRecords.ContainsKey(key))
                {
                    duplicates.Add(record);
                }
                else
                {
                    uniqueRecords[key] = record;
                }
            }

            _logger.LogInformation($"Finished processing {recordCount} records. Found {uniqueRecords.Count} unique records and {duplicates.Count} duplicates");
            return (uniqueRecords.Values, duplicates);
        }
    }
}
