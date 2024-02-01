using ClosedXML.Excel;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLServerSync;
public class SyncProcessor
{
    private SyncConfig config;
    public SyncProcessor(SyncConfig config)
    {
        this.config = config;
    }

    #region read and write CSV file

    /// <summary>persist the `data`(<see cref="DataTable"/> type) into CSV file</summary>
    void persistDataTableIntoCSVFile(string filePath, DataTable data)
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
    DataTable readDataFromCSVFile(string filePath)
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
    void persistDataTableIntoExcelFile(string filePath, DataTable data)
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
    DataTable readDataFromExcelFile(string filePath)
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

    void truncateDataTableInSQLServer(string connectionString, string table)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        using SqlCommand cmd = connection.CreateCommand();
        cmd.CommandText = $"TRUNCATE TABLE {table};";
        cmd.ExecuteNonQuery();
    }

    /// <summary>export the source table as <see cref="DataTable"/> from SQL Server</summary>
    DataTable exportDataFromSQLServer(string connectionString, string table)
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
    void persistDataTableIntoSQLServer(string connectionString, string table, DataTable data)
    {
        if (data == null || data.Rows.Count == 0)
            return;
        //using var connection = new SqlConnection(connectionString);
        //connection.Open();
        //using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection);
        using Microsoft.Data.SqlClient.SqlBulkCopy bulkCopy = new Microsoft.Data.SqlClient.SqlBulkCopy(connectionString, Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity);
        string defaultOrderHint = "id";
        if (data.Columns.Contains(defaultOrderHint))
        {
            // https://learn.microsoft.com/en-us/sql/connect/ado-net/sql/bulk-copy-order-hints
            // beware of letter case
            int idxOfOrderHint = data.Columns.IndexOf(defaultOrderHint);
            string realName = data.Columns[idxOfOrderHint].ColumnName;
            Microsoft.Data.SqlClient.SqlBulkCopyColumnOrderHint hintNumber =
                new Microsoft.Data.SqlClient.SqlBulkCopyColumnOrderHint(realName, Microsoft.Data.SqlClient.SortOrder.Ascending);
            bulkCopy.ColumnOrderHints.Add(hintNumber);
        }
        bulkCopy.DestinationTableName = table;
        bulkCopy.BatchSize = 1024;

        bulkCopy.WriteToServer(data);
    }

    #endregion

    /// <summary>main business logic</summary>
    public void Process()
    {
        foreach (var table in config.TableList)
        {
            var filePath = Path.Combine(config.FileFolder, $"{table}_{DateTime.Now.ToString("yyyyMMdd")}.{config.FileExtension}");
            if (config.Mode == SyncMode.Export)
            {
                Console.WriteLine($"exporting [{table}]");
                // exporting data from source table
                DataTable data = exportDataFromSQLServer(config.SourceDB, table);
                if (data == null || data.Rows.Count == 0)
                {
                    Console.WriteLine("empty source table, continue...");
                }
                else
                {
                    Console.WriteLine($"saving data into file [{filePath}]");
                    // persist the data into csv/excel file
                    if (config.FileFormat == SyncFileFormat.CSV)
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
            else if (config.Mode == SyncMode.Import)
            {
                // importing data into destination table
                if (!File.Exists(filePath))
                {
                    Console.WriteLine($"exported file does NOT exist [{filePath}]");
                }
                else
                {
                    Console.WriteLine($"reading data from file [{filePath}]");
                    DataTable data = config.FileFormat == SyncFileFormat.CSV ? readDataFromCSVFile(filePath) : readDataFromExcelFile(filePath);
                    if (data == null || data.Rows.Count == 0)
                    {
                        Console.WriteLine("empty csv file, continue...");
                    }
                    else
                    {
                        Console.WriteLine($"importing into [{table}]");
                        // CAUTION: truncate destination table first
                        truncateDataTableInSQLServer(config.DestinationDB, table);
                        // write the data into destination table
                        persistDataTableIntoSQLServer(config.DestinationDB, table, data);
                        Console.WriteLine($"Done [{table}]");
                    }
                }
            }
        }
        Console.WriteLine("✔️✔️ Winner Winner, Chicken Dinner!");
        Console.WriteLine("You may close the window to exit, or leave it alone and wait for next execution.");
    }
}
