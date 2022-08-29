
# Background

This repo was created due to the requirement of synchronous data between two SQL Server instances which was located in two isolated environment (can NOT connect to each other directly).

# How to use

1. [Download](https://github.com/ZXS66/SQLServerSync/releases) the executable file (or you can clone this repo and build the file yourself)
2. Configure the app in [App.config](#Sample-App-config). Supporting arguments as below:
    - `mode`: e/export or i/import
    - `source`: source table to be exported
    - `destination`: destination table to be imported
    - `sourceDB` connection string: connection string of source database, refer to [this manual](https://www.connectionstrings.com/sql-server/) for more details
    - `destinationDB` connection string: connection string of destination database, refer to [this manual](https://www.connectionstrings.com/sql-server/) for more details
3. Run the app (twice times) within the isolated environments:
    ```
    ./SQLServerSync.exe
    ```
    **CAUTION: the command will truncate destination table before importing, please backup the destination table before running the app!!!**

# Sample App.config

```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
	<appSettings>
		<!--<add key="syncMode" value="export"/>-->
		<add key="syncMode" value="import"/>
		<add key="source" value="your_source_table"/>
		<add key="destination" value="your_destination_table"/>
	</appSettings>
	<connectionStrings>
		<add name="sourceDB" connectionString="Data Source=xxxx;Initial Catalog=your_source_db;user id=your_user_name;password=your_password"/>
		<add name="destinationDB" connectionString="Data Source=yyyy;Initial Catalog=your_destination_db;user id=your_user_name;password=your_password"/>
	</connectionStrings>
</configuration>
```

# Methodology

1. First the app will export all data of source table into a CSV file (located in `./data` folder)
2. Then manually copy the CSV file from source environment to destination environment
3. Lastly import the data into destination table by running the app again
