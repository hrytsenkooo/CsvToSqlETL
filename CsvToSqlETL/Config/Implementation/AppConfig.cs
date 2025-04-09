using CsvToSqlETL.Config.Interface;
using Microsoft.Extensions.Configuration;

namespace CsvToSqlETL.Config.Implementation
{
    public class AppConfig : IAppConfig
    {
        public string CsvPath { get; }
        public string DbConnectionString { get; }
        public string DuplicatesCsvPath { get; }
        public int BatchSize { get; }

        public AppConfig(IConfiguration configuration)
        {
            try
            { 
                CsvPath = GetRequired(configuration, "CSV_PATH");
                DbConnectionString = GetRequired(configuration, "DB_CONNECTION_STRING");
                DuplicatesCsvPath = GetValueOrDefault(configuration, "DUPLICATES_CSV_PATH", "duplicates.csv");
                BatchSize = GetValueOrDefault(configuration, "BATCH_SIZE", 1000);
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Error loading configuration: {ex.Message}", ex);
            }
        }

        private string GetRequired(IConfiguration configuration, string key)
        { 
            string value = configuration[key] ?? Environment.GetEnvironmentVariable(key);
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException($"Missing required configuration value: {key}");
            }
            return value;
        }

        private string GetValueOrDefault(IConfiguration configuration, string key, string defaultValue)
        {
            string value = configuration[key] ?? Environment.GetEnvironmentVariable(key);
            return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
        }

        private int GetValueOrDefault(IConfiguration configuration, string key, int defaultValue)
        {
            string value = configuration[key] ?? Environment.GetEnvironmentVariable(key);
            return int.TryParse(value, out int result) ? result : defaultValue;
        }
    }
}
