using CsvToSqlETL.Models;

namespace CsvToSqlETL.Services.Interfaces
{
    public interface IDatabaseService
    {
        Task CreateSchemaIfNotExistsAsync();
        Task BulkInsertAsync(IEnumerable<TripRecord> records);
        Task<int> GetRowCountAsync();
    }
}
