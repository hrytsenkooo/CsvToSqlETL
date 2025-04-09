using CsvToSqlETL.Config.Implementation;
using CsvToSqlETL.Config.Interface;
using CsvToSqlETL.Services.Implementations;
using CsvToSqlETL.Services.Interfaces;
using DotNetEnv;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CsvToSqlETL
{
    class Program
    {
        static async Task Main(string[] args)
        {

            var serviceProvider = ConfigureServices();

            try
            {
                Console.WriteLine("Starting ETL process...");

                var etlService = serviceProvider.GetRequiredService<IEtlService>();
                var result = await etlService.ProcessDataAsync();

                Console.WriteLine("ETL process completed successfully");
                Console.WriteLine($"Processed {result.ProcessedRows} rows.");
                Console.WriteLine($"Found {result.DuplicatesCount} duplicate records");
            }
            catch (Exception ex)
            {
                var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
                logger.LogError(ex, "An error occurred during ETL process");
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static ServiceProvider ConfigureServices()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IAppConfig, AppConfig>();

            services.AddTransient<ICsvReader, CsvReader>();
            services.AddTransient<IDatabaseService, DatabaseService>();
            services.AddTransient<IDataProcessor, DataProcessor>();
            services.AddTransient<IEtlService, EtlService>();

            services.AddLogging(configure => configure.AddConsole());

            return services.BuildServiceProvider();
        }
    }
}