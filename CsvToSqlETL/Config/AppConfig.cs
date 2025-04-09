using DotNetEnv;

namespace CsvToSqlETL.Config
{
    public static class AppConfig
    {
        public static readonly string CsvPath;
        public static readonly string DbConnectionString;

        static AppConfig()
        {
            try
            {
                Env.Load();
                CsvPath = Env.GetString("CSV_PATH");
                DbConnectionString = Env.GetString("DB_CONNECTION_STRING");

                if (string.IsNullOrWhiteSpace(CsvPath))
                {
                    throw new Exception("Missing CSV_PATH configuration value.");
                }

                if (string.IsNullOrWhiteSpace(DbConnectionString))
                {
                    throw new Exception("Missing DB_CONNECTION_STRING configuration value.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading environment variables: {ex.Message}");
                Environment.Exit(1);
            }
        }
    }
}
