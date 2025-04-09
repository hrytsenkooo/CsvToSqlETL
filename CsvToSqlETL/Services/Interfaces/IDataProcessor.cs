using CsvToSqlETL.Models;

namespace CsvToSqlETL.Services.Interfaces
{
    public interface IDataProcessor
    {
        Task<(IEnumerable<TripRecord> UniqueRecords, IEnumerable<TripRecord> Duplicates)> ProcessRecordsAsync();
    }
}
