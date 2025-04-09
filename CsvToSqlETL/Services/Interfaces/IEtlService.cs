using CsvToSqlETL.Models;

namespace CsvToSqlETL.Services.Interfaces
{
    public interface IEtlService
    {
        Task<EtlResult> ProcessDataAsync();
    }
}
