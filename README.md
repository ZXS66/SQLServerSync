
# Background

This repo is aim to the synchronize data between two SQL Server instances, which were located in two isolated environments (can NOT communicate with each other directly).

# How to use

1. [Download](https://github.com/ZXS66/SQLServerSync/releases) the executable file (or you can clone this repo and build the file yourself)
2. Configure the app (twice times) in [App.config](#sample-appconfig). Supporting arguments as below:
    - `syncMode`: e/export or i/import
    - `table`: source/destination table(s) to be exported/imported
	- `fileFormat`: the extension of exported file, available options: `csv`, `excel`
	- `fileFolder`: the folder that exported file to be saved (`./data` if not specified)
	- `intervalInSecond`: the interval (in seconds) to re-run the app
    - `sourceDB` connection string: connection string of source database, refer to [this manual](https://www.connectionstrings.com/sql-server/) for more details
    - `destinationDB` connection string: connection string of destination database, refer to [this manual](https://www.connectionstrings.com/sql-server/) for more details
3. Run the app (twice times) within the isolated environments (double click the `exe` file or type below command in Windows Terminal/Powershell):
    ```
    .\SQLServerSync.exe
    ```
    **CAUTION: the command will truncate destination table before importing, please backup the destination table before running the app!!!**

# Sample App.config

```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
	<appSettings>
		<!--<add key="syncMode" value="export"/>-->
		<add key="syncMode" value="import"/>
		<add key="table" value="your_table1,your_table2,your_table3"/>
		<add key="fileFormat" value="excel"/>
		<add key="fileFolder" value="\\path\to\saved\your\files"/>
		<add key="intervalInSecond" value="86400"/>
	</appSettings>
	<connectionStrings>
		<add name="sourceDB" connectionString="Data Source=xxxx;Initial Catalog=your_source_db;user id=your_user_name;password=your_password"/>
		<add name="destinationDB" connectionString="Data Source=yyyy;Initial Catalog=your_destination_db;user id=your_user_name;password=your_password"/>
	</connectionStrings>
</configuration>
```

# Methodology

1. First the app will export all data of source table into a CSV/xlsx file, the exported file will be saved `fileFolder` if specified, otherwise save to default folder `./data`
2. Then manually copy the CSV/xlsx file from source environment to destination environment
3. Lastly import the data into destination table by running the app again
