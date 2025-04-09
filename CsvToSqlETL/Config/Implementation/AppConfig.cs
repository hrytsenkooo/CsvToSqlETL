using CsvToSqlETL.Config.Interface;
using DotNetEnv;

namespace CsvToSqlETL.Config.Implementation
{
    public class AppConfig : IAppConfig
    {
        public string CsvPath { get; }
        public string DbConnectionString { get; }
        public string DuplicatesCsvPath { get; }
        public int BatchSize { get; }

        public AppConfig()
        {
            try
            {
                Env.Load();

                CsvPath = GetRequiredEnvVariable("CSV_PATH");
                DbConnectionString = GetRequiredEnvVariable("DB_CONNECTION_STRING");
                DuplicatesCsvPath = GetEnvVariableOrDefault("DUPLICATES_CSV_PATH", "duplicates.csv");

                if (!int.TryParse(Environment.GetEnvironmentVariable("BATCH_SIZE"), out int batchSize))
                {
                    batchSize = 1000;
                }
                BatchSize = batchSize;
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Error loading configuration: {ex.Message}", ex);
            }
        }

        private string GetRequiredEnvVariable(string name)
        { 
            string value = Environment.GetEnvironmentVariable(name);
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException($"Missing required environment variable: {name}");
            }

            return value;
        }

        private string GetEnvVariableOrDefault(string name, string defaultValue)
        {
            string value = Environment.GetEnvironmentVariable(name);
            return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
        }
    }
}
