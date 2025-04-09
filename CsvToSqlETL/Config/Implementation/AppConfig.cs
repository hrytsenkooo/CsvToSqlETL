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

        /// <summary>
        /// Initializes a new instance of the AppConfig class, retrieving required and optional configuration values.
        /// </summary>
        /// <param name="configuration">The configuration provider</param>
        /// <exception cref="ApplicationException">Thrown when required configuration is missing or invalid</exception>
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

        /// <summary>
        /// Retrieves a required configuration value. Throws if not found or empty.
        /// </summary>
        /// <param name="configuration">The configuration provider</param>
        /// <param name="key">The key of the required configuration value</param>
        /// <returns>The value associated with the key</returns>
        /// <exception cref="ArgumentException">Thrown if the value is missing or empty</exception>
        private string GetRequired(IConfiguration configuration, string key)
        { 
            string value = configuration[key] ?? Environment.GetEnvironmentVariable(key);
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException($"Missing required configuration value: {key}");
            }
            return value;
        }

        /// <summary>
        /// Retrieves an optional string configuration value, returning a default if not found
        /// </summary>
        /// <param name="configuration">The configuration provider</param>
        /// <param name="key">The configuration key</param>
        /// <param name="defaultValue">The default value to use if none is found</param>
        /// <returns>The configuration value or the default</returns>
        private string GetValueOrDefault(IConfiguration configuration, string key, string defaultValue)
        {
            string value = configuration[key] ?? Environment.GetEnvironmentVariable(key);
            return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
        }

        /// <summary>
        /// Retrieves an optional integer configuration value, returning a default if not found or invalid.
        /// </summary>
        /// <param name="configuration">The configuration provider</param>
        /// <param name="key">The configuration key</param>
        /// <param name="defaultValue">The default value to use if parsing fails or the value is missing</param>
        /// <returns>The configuration value or the default</returns>
        private int GetValueOrDefault(IConfiguration configuration, string key, int defaultValue)
        {
            string value = configuration[key] ?? Environment.GetEnvironmentVariable(key);
            return int.TryParse(value, out int result) ? result : defaultValue;
        }
    }
}
