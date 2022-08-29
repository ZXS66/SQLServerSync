using CsvHelper;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

/// <summary>convert string value into <see cref="SyncMode"/> (enum)</summary>
static SyncMode parseSyncMode(string? value)
{
    if (string.IsNullOrEmpty(value))
        throw new ArgumentNullException(nameof(value));
    string normValue;
    switch (value.Trim().ToUpper())
    {
        case "E":
        case "EXPORT":
            normValue = "Export";
            break;
        case "I":
        case "IMPORT":
            normValue = "Import";
            break;
        default:
            normValue = string.Empty;
            break;
    }
    if (string.IsNullOrEmpty(normValue))
        throw new ArgumentOutOfRangeException(nameof(value));
    return (SyncMode)Enum.Parse(typeof(SyncMode), value, true);
};

/// <summary>export the source table as <see cref="DataTable"/> from SQL Server</summary>
static DataTable exportDataFromSQLServer(string connectionString, string table)
{
    using (var connection = new SqlConnection(connectionString))
    {
        connection.Open();
        using (SqlCommand cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"SELECT * FROM {table}(NOLOCK)";
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                DataTable dt = new DataTable();
                dt.Load(reader);
                return dt;
            }
        }
    }
}
/// <summary>persist the `data`(<see cref="DataTable"/> type) into CSV file</summary>
static void persistDataTableIntoCSVFile(string filePath, DataTable data)
{
    if (File.Exists(filePath)) File.Delete(filePath);
    using (var writer = new StreamWriter(filePath))
    {
        using (var csv = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture))
        {
            //csv.WriteRecords(data.AsEnumerable());

            var columns = data.Columns;
            // Write columns
            foreach (DataColumn column in columns)
            {
                csv.WriteField(column.ColumnName);
            }
            csv.NextRecord();

            // Write row values
            foreach (DataRow row in data.Rows)
            {
                for (var i = 0; i < columns.Count; i++)
                {
                    csv.WriteField(row[i]);
                }
                csv.NextRecord();
            }
            csv.Flush();
        }
    }
}

/// <summary>read the source table as <see cref="DataTable"/> from CSV file</summary>
static DataTable readDataFromCSVFile(string filePath)
{
    using (var reader = new StreamReader(filePath))
    {
        using (var csv = new CsvReader(reader, System.Globalization.CultureInfo.InvariantCulture))
        {
            // do any configuration to `CsvReader` before creating CsvDataReader
            using (var dr = new CsvDataReader(csv))
            {
                DataTable dt = new DataTable();
                dt.Load(dr);
                return dt;
            }
        }
    }
}

static void truncateDataTableInSQLServer(string connectionString, string table)
{
    using (var connection = new SqlConnection(connectionString))
    {
        connection.Open();
        using (SqlCommand cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"TRUNCATE TABLE {table};";
            cmd.ExecuteNonQuery();
        }
    }
}

/// <summary>persist the `data`(<see cref="DataTable"/> type) into the destination (SQL Server) table</summary>
static void persistDataTableIntoSQLServer(string connectionString, string table, DataTable data)
{
    using (var connection = new SqlConnection(connectionString))
    {
        connection.Open();
        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
        {
            bulkCopy.DestinationTableName = table;
            bulkCopy.BatchSize = 1024;

            bulkCopy.WriteToServer(data);
        }
    }
}

#region main business logic here

const string dataFolder = "./data";

var syncModeStr = ConfigurationManager.AppSettings["syncMode"];
SyncMode thisSyncMode = parseSyncMode(syncModeStr);
var sourceTable = ConfigurationManager.AppSettings["source"];
var destinationTable = ConfigurationManager.AppSettings["destination"];
var sourceDB = ConfigurationManager.ConnectionStrings["sourceDB"]?.ConnectionString;
var destinationDB = ConfigurationManager.ConnectionStrings["destinationDB"]?.ConnectionString;

/// assertion all the arguments are specified
if (string.IsNullOrEmpty(sourceTable)) throw new ArgumentNullException(nameof(sourceTable));
if (string.IsNullOrEmpty(destinationTable)) throw new ArgumentNullException(nameof(destinationTable));
if (string.IsNullOrEmpty(sourceDB)) throw new ArgumentNullException(nameof(sourceDB));
if (string.IsNullOrEmpty(destinationDB)) throw new ArgumentNullException(nameof(destinationDB));

if (!Directory.Exists(dataFolder)) Directory.CreateDirectory(dataFolder);
/// <summary>the path of the csv file for transferring data</summary>
var filePath = Path.Combine(dataFolder, $"{sourceTable}_{DateTime.Now.ToString("yyyyMMdd")}.csv");
if (thisSyncMode == SyncMode.Export)
{
    // exporting data from source table
    DataTable data = exportDataFromSQLServer(sourceDB, sourceTable);
    if (data == null || data.Rows.Count == 0)
    {
        Console.WriteLine("empty source table, existing program...");
    }
    else
    {
        // persist the data into csv file
        persistDataTableIntoCSVFile(filePath, data);
    }
}
else if (thisSyncMode == SyncMode.Import)
{
    // importing data into destination table
    DataTable data = readDataFromCSVFile(filePath);
    if (data == null || data.Rows.Count == 0)
    {
        Console.WriteLine("empty csv file, existing program...");
    }
    else
    {
        // CAUTION: truncate destination table first
        truncateDataTableInSQLServer(destinationDB, destinationTable);
        // write the data into destination table
        persistDataTableIntoSQLServer(destinationDB, destinationTable, data);
    }
}

Console.WriteLine("press any key to continue...");
Console.ReadKey();

#endregion

/// <summary>synchronous mode of the app</summary>
public enum SyncMode
{
    /// <summary>export the data from source table</summary>
    Export,
    /// <summary>import the data into destination table</summary>
    Import
}
