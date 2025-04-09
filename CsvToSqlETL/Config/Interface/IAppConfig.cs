namespace CsvToSqlETL.Config.Interface
{
    public interface IAppConfig
    {
        string CsvPath { get; }
        string DbConnectionString { get; }
        string DuplicatesCsvPath { get; }    
    }
}
