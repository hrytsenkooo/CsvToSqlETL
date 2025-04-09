using CsvHelper.Configuration;
using CsvToSqlETL.Config.Interface;
using CsvToSqlETL.Models;
using CsvToSqlETL.Services.Interfaces;
using Microsoft.Extensions.Logging;
using System.Globalization;


namespace CsvToSqlETL.Services.Implementations
{
    public class CsvReader : ICsvReader
    {
        private readonly IAppConfig _config;
        private readonly ILogger<CsvReader> _logger;

        public CsvReader(IAppConfig config, ILogger<CsvReader> logger)
        {
            _config = config;
            _logger = logger;
        }

        public async IAsyncEnumerable<TripRecord> ReadRecordAsync()
        {
            _logger.LogInformation($"Reading data from {_config.CsvPath}");

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                MissingFieldFound = null,
                BadDataFound = context =>
                {
                    _logger.LogWarning($"Bad data found at row {context.RawRecord}");
                }
            };

            using var reader = new StreamReader(_config.CsvPath);
            using var csv = new CsvHelper.CsvReader(reader, config);

            await csv.ReadAsync();
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                TripRecord record;

                try
                {
                    record = csv.GetRecord<TripRecord>();

                    record.StoreAndFwdFlag = record.StoreAndFwdFlag?.Trim() switch
                    {
                        "Y" => "Yes",
                        "N" => "No",
                        _ => record.StoreAndFwdFlag?.Trim() ?? string.Empty
                    };
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error parsing record at row {csv.Context.Parser.Row}");
                    continue;
                }

                yield return record;
            }
        }

        public async Task WriteDuplicatesToCsvAsync(IEnumerable<TripRecord> duplicates)
        {
            _logger.LogInformation($"Writing {duplicates} duplicates to {_config.DuplicatesCsvPath}");

            using var writer = new StreamWriter(_config.DuplicatesCsvPath);
            using var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture);

            await csv.WriteRecordsAsync(duplicates);
        }
    }
}
