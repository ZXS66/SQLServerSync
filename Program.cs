using ClosedXML.Excel;
using CsvHelper;
// using Microsoft.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;


if (args.Length > 0 && (args.ElementAt(0) == "-forever" || args.ElementAt(0) == "--forever"))
{
    runForever();
}
else
{
    runOnce();
}


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

#region read and write CSV file

/// <summary>persist the `data`(<see cref="DataTable"/> type) into CSV file</summary>
static void persistDataTableIntoCSVFile(string filePath, DataTable data)
{
    if (File.Exists(filePath)) File.Delete(filePath);
    using var writer = new StreamWriter(filePath);
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

/// <summary>read the source table as <see cref="DataTable"/> from CSV file</summary>
static DataTable readDataFromCSVFile(string filePath)
{
    using var reader = new StreamReader(filePath);
    using var csv = new CsvReader(reader, System.Globalization.CultureInfo.InvariantCulture);
    // do any configuration to `CsvReader` before creating CsvDataReader
    using var dr = new CsvDataReader(csv);
    DataTable dt = new DataTable();
    dt.Load(dr);
    return dt;
}

#endregion

#region read and write Excel file

/// <summary>persist the `data`(<see cref="DataTable"/> type) into excel file</summary>
static void persistDataTableIntoExcelFile(string filePath, DataTable data)
{
    if (data != null && data.Rows.Count > 0)
    {
        using XLWorkbook wb = new XLWorkbook();
        //Add DataTable in worksheet
        string fileName = Path.GetFileNameWithoutExtension(filePath);
        wb.Worksheets.Add(data, fileName.Substring(0, Math.Min(fileName.Length, 31)));
        wb.SaveAs(filePath);
    }
}

/// <summary>read the source table as <see cref="DataTable"/> from excel file</summary>
static DataTable readDataFromExcelFile(string filePath)
{
    using XLWorkbook wb = new XLWorkbook(filePath);
    //var sheet = wb.Worksheet(0); excel index starts with 1
    var sheet = wb.Worksheet(1);

    DataTable dt = new DataTable();

    bool initialized = false;
    foreach (var row in sheet.Rows())
    {
        if (!initialized)
        {
            foreach (IXLCell cell in row.Cells())
            {
                dt.Columns.Add(cell.Value.ToString());
            }
            initialized = true;
        }
        else
        {
            //Adding rows to DataTable.
            dt.Rows.Add();
            foreach (IXLCell cell in row.Cells())
            {
                dt.Rows[dt.Rows.Count - 1][cell.Address.ColumnNumber - 1] = cell.Value.ToString();
            }
        }
    }

    return dt;
}

#endregion

#region SQL Server query

static void truncateDataTableInSQLServer(string connectionString, string table)
{
    using var connection = new SqlConnection(connectionString);
    connection.Open();
    using SqlCommand cmd = connection.CreateCommand();
    cmd.CommandText = $"TRUNCATE TABLE {table};";
    cmd.ExecuteNonQuery();
}

/// <summary>export the source table as <see cref="DataTable"/> from SQL Server</summary>
static DataTable exportDataFromSQLServer(string connectionString, string table)
{
    using var connection = new SqlConnection(connectionString);
    connection.Open();
    using SqlCommand cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM {table}(NOLOCK)";
    using SqlDataReader reader = cmd.ExecuteReader();
    DataTable dt = new DataTable();
    dt.Load(reader);
    return dt;
}

/// <summary>persist the `data`(<see cref="DataTable"/> type) into the destination (SQL Server) table</summary>
static void persistDataTableIntoSQLServer(string connectionString, string table, DataTable data)
{
    if (data == null || data.Rows.Count == 0)
        return;
    //using var connection = new SqlConnection(connectionString);
    //connection.Open();
    //using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection);
    using Microsoft.Data.SqlClient.SqlBulkCopy bulkCopy = new Microsoft.Data.SqlClient.SqlBulkCopy(connectionString, Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity);
    if (data.Columns.Contains("id"))
    {
        // https://learn.microsoft.com/en-us/sql/connect/ado-net/sql/bulk-copy-order-hints
        // Setup an order hint for the ProductNumber column.
        Microsoft.Data.SqlClient.SqlBulkCopyColumnOrderHint hintNumber =
            new Microsoft.Data.SqlClient.SqlBulkCopyColumnOrderHint("id", Microsoft.Data.SqlClient.SortOrder.Ascending);
        bulkCopy.ColumnOrderHints.Add(hintNumber);
    }
    bulkCopy.DestinationTableName = table;
    bulkCopy.BatchSize = 1024;

    bulkCopy.WriteToServer(data);
}

#endregion

static void runForever()
{

    do
    {
        runOnce();
        //Console.WriteLine("Congratulations!");
        Console.WriteLine("Congratulations!!!");
        int interval = int.Parse(ConfigurationManager.AppSettings["intervalInSecond"] ?? "86400");
        Console.WriteLine("next processing time: " + DateTime.Now.AddSeconds(interval));
        Thread.Sleep(interval * 1000);
    } while (true);
}

static void runOnce()
{
    try
    {
        process();
    }
    catch (Exception e)
    {
        Console.WriteLine(e?.InnerException?.Message ?? e?.Message);
        Console.WriteLine("press any key to continue...");
    }
}

/// <summary>main business logic</summary>
static void process()
{
    var syncModeStr = ConfigurationManager.AppSettings["syncMode"];
    SyncMode thisSyncMode = parseSyncMode(syncModeStr);
    var tables = ConfigurationManager.AppSettings["table"];
    var _fileFormat = ConfigurationManager.AppSettings["fileFormat"];
    var fileFolder = ConfigurationManager.AppSettings["fileFolder"];
    var sourceDB = ConfigurationManager.ConnectionStrings["sourceDB"]?.ConnectionString;
    var destinationDB = ConfigurationManager.ConnectionStrings["destinationDB"]?.ConnectionString;

    /// assertion all the arguments are specified
    if (string.IsNullOrEmpty(tables)) throw new ArgumentNullException(nameof(tables));
    var tableList = tables.Split(',', StringSplitOptions.RemoveEmptyEntries);
    if (string.IsNullOrEmpty(sourceDB)) throw new ArgumentNullException(nameof(sourceDB));
    if (string.IsNullOrEmpty(destinationDB)) throw new ArgumentNullException(nameof(destinationDB));

    FileFormat fileFormat = (!string.IsNullOrWhiteSpace(_fileFormat) && _fileFormat.Trim().ToUpper() == "CSV") ? FileFormat.CSV : FileFormat.Excel;
    fileFolder ??= "./data";

    if (!Directory.Exists(fileFolder)) Directory.CreateDirectory(fileFolder);
    // the path of the csv/excel file for transferring data
    string fileExtension = fileFormat == FileFormat.CSV ? "csv" : "xlsx";

    foreach (var table in tableList)
    {
        var filePath = Path.Combine(fileFolder, $"{table}_{DateTime.Now.ToString("yyyyMMdd")}.{fileExtension}");
        if (thisSyncMode == SyncMode.Export)
        {
            Console.WriteLine($"exporting [{table}]");
            // exporting data from source table
            DataTable data = exportDataFromSQLServer(sourceDB, table);
            if (data == null || data.Rows.Count == 0)
            {
                Console.WriteLine("empty source table, continue...");
            }
            else
            {
                Console.WriteLine($"saving data into file [{filePath}]");
                // persist the data into csv/excel file
                if (fileFormat == FileFormat.CSV)
                {
                    persistDataTableIntoCSVFile(filePath, data);
                }
                else
                {
                    persistDataTableIntoExcelFile(filePath, data);
                }
                Console.WriteLine($"Done [{table}]");
            }
        }
        else if (thisSyncMode == SyncMode.Import)
        {
            // importing data into destination table
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"exported file does NOT exist [{filePath}]");
            }
            else
            {
                Console.WriteLine($"reading data from file [{filePath}]");
                DataTable data = fileFormat == FileFormat.CSV ? readDataFromCSVFile(filePath) : readDataFromExcelFile(filePath);
                if (data == null || data.Rows.Count == 0)
                {
                    Console.WriteLine("empty csv file, continue...");
                }
                else
                {
                    Console.WriteLine($"importing into [{table}]");
                    // CAUTION: truncate destination table first
                    truncateDataTableInSQLServer(destinationDB, table);
                    // write the data into destination table
                    persistDataTableIntoSQLServer(destinationDB, table, data);
                    Console.WriteLine($"Done [{table}]");
                }
            }
        }
    }
}


/// <summary>synchronous mode of the app</summary>
public enum SyncMode
{
    /// <summary>export the data from source table</summary>
    Export,
    /// <summary>import the data into destination table</summary>
    Import
}

/// <summary>supported files that </summary>
public enum FileFormat
{
    CSV,
    Excel
}