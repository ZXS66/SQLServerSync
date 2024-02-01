using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLServerSync;
public class SyncConfig
{
    public SyncMode Mode { get; private set; }
    public SyncFileFormat FileFormat { get; private set; }
    public IEnumerable<string> TableList { get; private set; }
    public string FileFolder { get; private set; }
    /// <summary>
    /// the extension of the csv/excel file for transferring data
    /// </summary>
    public string FileExtension
    {
        get
        {
            return this.FileFormat == SyncFileFormat.CSV ? "csv" : "xlsx";
        }
    }
    public string SourceDB { get; private set; }
    public string DestinationDB { get; private set; }

    public SyncConfig()
    {
        var syncModeStr = ConfigurationManager.AppSettings["syncMode"];
        var tables = ConfigurationManager.AppSettings["table"];
        var _fileFormat = ConfigurationManager.AppSettings["fileFormat"];
        this.FileFolder = ConfigurationManager.AppSettings["fileFolder"];
        this.SourceDB = ConfigurationManager.ConnectionStrings["sourceDB"]?.ConnectionString;
        this.DestinationDB = ConfigurationManager.ConnectionStrings["destinationDB"]?.ConnectionString;

        /// assertion all the arguments are specified
        this.Mode = parseSyncMode(syncModeStr);
        if (string.IsNullOrEmpty(tables)) throw new ArgumentNullException(nameof(tables));
        this.TableList = tables.Split(',', StringSplitOptions.RemoveEmptyEntries);
        if (string.IsNullOrEmpty(this.SourceDB)) throw new ArgumentNullException(nameof(this.SourceDB));
        if (string.IsNullOrEmpty(this.DestinationDB)) throw new ArgumentNullException(nameof(this.DestinationDB));

        this.FileFormat = (!string.IsNullOrWhiteSpace(_fileFormat) && _fileFormat.Trim().ToUpper() == "CSV") ? SyncFileFormat.CSV : SyncFileFormat.Excel;
        this.FileFolder ??= "./data";

        if (!Directory.Exists(this.FileFolder)) Directory.CreateDirectory(this.FileFolder);
    }

    /// <summary>convert string value into <see cref="SyncMode"/> (enum)</summary>
    private SyncMode parseSyncMode(string? value)
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

/// <summary>supported files that can be exported/imported</summary>
public enum SyncFileFormat
{
    CSV,
    Excel
}
