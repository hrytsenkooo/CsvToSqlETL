using CsvHelper.Configuration;
using CsvToSqlETL.Config.Interface;
using CsvToSqlETL.Models;
using CsvToSqlETL.Services.Interfaces;
using CsvToSqlETL.Validators;
using Microsoft.Extensions.Logging;
using System.Globalization;


namespace CsvToSqlETL.Services.Implementations
{
    public class CsvReader : ICsvReader
    {
        private readonly IAppConfig _config;
        private readonly ILogger<CsvReader> _logger;

        /// <summary>
        /// Initializes a new instance of the CsvReader class with the specified configuration and logger
        /// </summary>
        /// <param name="config">Application configuration containing file paths and settings</param>
        /// <param name="logger">Logger for diagnostic information</param>
        public CsvReader(IAppConfig config, ILogger<CsvReader> logger)
        {
            _config = config;
            _logger = logger;
        }

        /// <summary>
        /// Asynchronously reads trip records from the CSV file, validates each record,
        /// normalizes data (converts Y/N flags to Yes/No and removes whitespace),
        /// and yields valid records as an asynchronous stream
        /// </summary>
        /// <returns>An asynchronous enumerable of validated TripRecord objects</returns>
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

                    if (!TripRecordValidator.TryValidate(ref record, out var validationError))
                    {
                        _logger.LogWarning($"Invalid record at row {csv.Context.Parser.Row}: {validationError}");
                        continue;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error parsing record at row {csv.Context.Parser.Row}");
                    continue;
                }

                yield return record;
            }
        }

        /// <summary>
        /// Writes duplicate trip records to a CSV file for auditing purposes
        /// </summary>
        /// <param name="duplicates">Collection of duplicate trip records identified during processing</param>
        /// <returns>A task representing the asynchronous write operation</returns>
        public async Task WriteDuplicatesToCsvAsync(IEnumerable<TripRecord> duplicates)
        {
            _logger.LogInformation($"Writing {duplicates.Count()} duplicates to {_config.DuplicatesCsvPath}");

            using var writer = new StreamWriter(_config.DuplicatesCsvPath);
            using var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture);

            await csv.WriteRecordsAsync(duplicates);
        }
    }
}
