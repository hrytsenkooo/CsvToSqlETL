using CsvToSqlETL.Models;

namespace CsvToSqlETL.Services.Interfaces
{
    public interface ICsvReader
    {
        IAsyncEnumerable<TripRecord> ReadRecordAsync();
        Task WriteDuplicatesToCsvAsync(IEnumerable<TripRecord> duplicates);
    }
}
