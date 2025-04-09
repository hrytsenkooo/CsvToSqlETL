namespace CsvToSqlETL.Models
{
    public class EtlResult
    {
        public int ProcessedRows { get; set; }
        public int DuplicatesCount { get; set; }
    }
}
