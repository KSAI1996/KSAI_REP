# Metadata-Driven Pipeline for Full and Incremental Load in Microsoft Fabric 

## <span style='color: Blue;'>Overview</span>:
This document outlines the setup and configuration of a metadata-driven pipeline for implementing both full and incremental data loads in Microsoft Fabric. The pipeline leverages metadata to dynamically manage the extraction, transformation, and loading (ETL) process, enabling flexible and efficient data integration workflows.

## <span style='color: Blue;'>Prerequisites</span>:
- Access to Microsoft Fabric.
- Familiarity with SQL and data manipulation.
- SQL Server Management Studio or Azure Data Studio
- Basic understanding of creating data pipelines, either from Azure Data Factory, Synapse Analytics or Microsoft Fabric.

## <span style='color: Blue;'>Key Components</span>:

- **Metadata Repository**: This is a storage location where metadata definitions for tables, columns, and load configurations are stored. It can be implemented using Azure SQL Database or any other suitable storage solution.
- **Metadata Table**: The table contain definitions for source and target tables, column mappings, load strategies (full or incremental), and any other relevant configurations.
- **Pipeline**: The pipeline is responsible for orchestrating the ETL process based on metadata configurations in metadata table. At the same time, the pipelines will update the metadata table with the latest run details gathered from the Fabric pipeline metadata such as rowsupdated, rowsinserted, etc. 
- **Spark Notebooks**: The Spark notebooks in Fabric are used to perform data transformations such as filtering, aggregating, and joining datasets.
- **SQL Queries**: SQL queries to create the metadata table and further use in various pipeline activities like Lookup, Copy, Script, etc. 

## <span style='color: Blue;'>Setup and Configuration</span>:

- **Define Metadata Table**: Create the metadata table structure for the [SQL script](/Accelerator_Resources/sql-metadata-table-script.txt) provided with this accelerator. Populate this table to store source and target tables, column mappings, load strategies, and any additional properties required for ETL processes. Below is the image of the metadata table. ![Image](Images/MDT_Table_row_screenshot.png)
- **Configure Pipeline**: Set up a pipeline in Fabric to read metadata from the metadata table and use this metadata to parameterise different parameters such as, sourcetablename, sinktablename, etc. in UpdateScript activity in the pipeline, copy activity, etc.
- **Implement Spark Notebooks**: Use [Spark Notebooks](/Accelerator_Resources/python-notebook-create-or-merge-to-deltalake.ipnyb) within pipeline to perform required transformations on the data.

## <span style='color: Blue;'>Architecture: End-to-End Metadata Driven Pipeline, From Source to Destination(Lakehouse)</span>:
![Architecture](Images/Architecture.png)

## <span style='color: Blue;'>Create Azure Resources
(This step is optional, if you already have an up and running SQL Database with read and write permission)</span>
#### 1. Create an Azure Resource Group
#### 2. Create an Azure SQL Server
- In the Azure Portal, in **Create a resource**, choose **SQL Server** and click **Create.**
- Choose your **Subscription**
- Choose you **Resource group** you created in the previous step
- Enter a **Server name** - note this must be unique across all azure
- Choose your **location**
- For authentication method, select **Use both SQL and Microsoft authentication**
- Click the **Set admin** button and select your user account
- Enter a **Server admin login and password**.
- Navigate to the **Networking** tab and change the slider under Firewall rules to **Yes** to allow Azure services and resources to access this server.
- Select **Review + create**
#### 3. Create an Azure SQL DB for Metadata Driven Pipeline Configurations
- Go to the Azure SQL Server you created in the previous step and click Create database
- Make sure your resource group and SQL server are selected. Enter FabricMetadataOrchestration for the database name.
- For Workload environment Choose Development
- Navigate to Networking and under Firewall rules, move slider for Add current client IP address to Yes
- Click Review and create

## Create the Metadata Table in Azure SQL DB
1. Use the script in [this repo](/Accelerator_Resources/sql-metadata-table-script.txt) to create the metadata table
2. Update the script to capture tables that you would like to ingest, columns named source_server and source_db can also be added if there are multiple servers and databases. 
3. Run the script in Azure Database to create a metadata table. 

## <span style='color: Blue;'>Create Fabric Resources</span>:
### Create a Microsoft Fabric Workspace
[Create a Fabric Workspace](https://learn.microsoft.com/en-us/fabric/get-started/create-workspaces) in your Microsoft Fabric tenant

### Create Bronze/Raw/Staging Fabric Lakehouse
[Create a Microsoft Fabric Lakehouses](https://learn.microsoft.com/en-us/fabric/data-engineering/create-lakehouse) in you workspace.
After creating the lakehouse, copy the lakehouse names and table URLs and keep for your reference. 
* To get the URLs, open each Lakehouse and click on the ellipses next to the **Tables** folder. Choose **Properties** and copy the abfss file path. ![getlakehouse](Images/Lakehouse_URL_Details.jpg)
* Paste each into notepad or a Word document and remove the "/Tables" from the end of the string. Your string will look something like **abfss://\<uniqueid>@onelake.dfs.fabric.microsoft.com/a\<anotheruniqueid>**

### Create Fabric Connections to your Azure SQL DBs
Create connections in Microsoft Fabric to all the sources from where data has to be imported. Also, create a connection to the Database/Server where the metadata table has been created. Please use the instructions if the metadata table has been created on Azure SQL DB [per the instructions here](https://learn.microsoft.com/en-us/fabric/data-factory/connector-azure-sql-database).

### Upload Spark Notebooks to Fabric

Upload the notebooks to be used in the pipeline
1. Download the notebook [found in the repo](/Accelerator_Resources/python-notebook-create-or-merge-to-deltalake.ipynb) which will be a part of the overall pipeline. 
1. Log into the Microsoft Fabric portal and switch to the Data Engineering experience and click **Import notebook**![Import Notebook](Images/Synapse_Notebook_Upload.png)
1. Select upload and choose the notebook to your Fabric Workspace. ![downloaded.](Images/Upload_Python_Notebook.png)

## <span style='color: Blue;'>Create Pipelines to Load Data from multiple sources to Fabric Lakehouse</span>
### Instructions for building the Data Pipelines
Two pipelines will be created:
1. Child Pipeline - This pipeline will be used for performing full and incremental load and to update the metadata table with the latest run statistics.
2. Orchestration Pipeline - This pipeline will read the metadata table to set various parameters such as sourcetablename, sinktablename, etc. and then call the child pipeline to pass these parameters. 

### Child Pipeline - Load to Lakehouse
Below are the instructions to setup the Child Pipeline:
Follow the instructions to create pipelines and add activities to configure the settings for each activity. The configurations for each activity are in a table that allows you to copy and paste values into each activity. It is important to copy the text exactly as is to avoid errors in scripts or subsequent activities.
This pipeline will be called from an Orchestrator pipeline to load tables from multiple sources to the Fabric Lakehouse. The pipeline will look like this when finished. 

![Child_Pipeline](Images/Child_Pipeline.png)

Create a new Data Pipeline and call it **"Load Data to Lakehouse (Child) Pipeline"**. Use the [Child-Pipeline](/Accelerator_Resources/load-to-lakehouse-(child)-pipeline.json) repo to copy the JSON code for the Child Pipeline (*this feature is not available in Microsoft Fabric at the time of writing the accelerator therefore, manual copy of steps is required*)
1. Add a Set variable activity
2. Click on the canvas and add the following pipeline **Parameters**:

 Name                | Type   |
 ------------------- | ------ |
 sqlsourcedatecolumn | String |
 sqlstartdate        | String |
 sqlenddate          | String |
 sqlsourceschema     | String |
 sqlsourcetable      | String |
 sinktablename       | String |
 loadtype            | String |
 tablekey            | String |
 tablekey2           | String |

3.  Move to the **Variables** tab and add the following variables: 

 | Name              | Type   |
 | ----------------- | ------ |
 | datepredicate     | String |
 | maxdate           | String |
 | rowsinserted      | String |
 | rowsupdated       | String |
 | pipelinestarttime | String |
 | pipelineendtime   | String |

4. Use the **Set Variable** Activity that was created initiattly and configure it as below:

 | Tab      | Configuration | Value Type         | Value                 |
 | -------- | ------------- | ------------------ | --------------------- |
 | General  | Name          | String             | Set pipelinestarttime |
 | Settings | Variable type | Radio Button       | Pipeline variable     |
 | Settings | Name          | String             | pipelinestarttime     |
 | Settings | Value         | Dynamic Content    | @utcnow()             |

5. Add **If condition** activity, drag arrow from previous activity and configure:

 | Tab        | Configuration | Value Type         | Value                                          |
 | ---------- | ------------- | ------------------ | ---------------------------------------------- |
 | General    | Name          | String             | Check loadtype                                 |
 | Activities | Expression    | Dynamic Content    | @equals(pipeline().parameters.loadtype,'full') |

6. Now configure the If True activities. Like the previous pipeline, the True activities will be a flow of activities when the table to be loaded is a full load. When completed, the True activities will look like this:

![If_True_Activities](Images/If_True_Activities.png)

i. Add **Copy Data** Activity and configure:
        
 | Tab         | Configuration             | Value Type         | Value                                 |
 | ----------- | ------------------------- | ------------------ | -----------------------------------   |
 | General     | Name                      | String             | Copy data to delta table              |
 | Source      | Data store type           | Radio Button       | External                              |
 | Source      | Workspace data store type | Drop down          | SQL Server(Data Source)               |
 | Source      | Data Warehouse            | Drop down          | \<choose your source connection>      |
 | Source      | Use query                 | Radio Button       | Table                                 |
 | Source      | Table (Schema)            | Dynamic Content    | @pipeline().parameters.sqlsourceschema|
 | Source      | Table (Table name)        | Dynamic Content    | @pipeline().parameters.sqlsourcetable |
 | Destination | Data store type           | Radio Button       | Workspace                             |
 | Destination | Workspace data store type | Drop down          | Lakehouse                             |
 | Destination | Lakehouse                 | Drop down          | \<choose your Fabric Lakehouse>       |
 | Destination | Root folder               | Radio Button       | Tables                                |
 | Destination | Table (Table name)        | Dynamic Content    | @pipeline().parameters.sinktablename  |
 | Destination | Advanced -> Table action  | Radio Button       | Overwrite                             |

ii. Add **Set variable** activity, drag the green arrow from the previous activity to it and configure:

 | Tab      | Configuration | Value Type         | Value                |
 | -------- | ------------- | ------------------ | -------------------- |
 | General  | Name          | String             | Set pipeline endtime |
 | Settings | Variable type | Radio Button       | Pipeline variable    |
 | Settings | Name          | Dropdown           | pipelineendtime      |
 | Settings | Value         | Dynamic Content    | @utcnow()            |
    
iii. Add **Script** activity, drag the green arrow from the previous activity to it and configure:

 | Tab      | Configuration   | Value Type      | Value                                             |
 | -------- | --------------- | ------------    | ------------------------------------------------- |
 | General  | Name            | String          | Update Pipeline Run details                       |
 | Settings | Data store type | Radio Button    | External                                          |
 | Settings | Connection      | Dropdown        | Connection to FabricMetdataOrchestration Database |
 | Settings | Script(1)       | Radio Button    | NonQuery                                          |
 | Settings | Script(2)       | Dynamic Content | Update dbo.PipelineOrchestrator_FabricLakehouse set batchloaddatetime = '@{pipeline().parameters.batchloaddatetime}', loadstatus = '@{activity('Copy data to delta table').output.executionDetails[0].status}', rowsread = @{activity('Copy data to delta table').output.rowsRead}, rowscopied= @{activity('Copy data to delta table').output.rowsCopied}, pipelinestarttime='@{variables('pipelinestarttime')}', pipelineendtime = '@{variables('pipelineendtime')}' where sqlsourceschema = '@{pipeline().parameters.sqlsourceschema}' and sqlsourcetable = '@{pipeline().parameters.sqlsourcetable}' |

iv. Exit the **True activities** box of the **If condition** by clicking on  **Main canvas** in the upper left corner

7. Now configure the **If False** activities. Your False activities will be a flow of activities when the table to be loaded should be an incremental load. When completed, the False activities will look like this: 
![If_False_Activities](Images/If_False_Activities.png)

i. Add **Set variable** activity and configure:

 | Tab      | Configuration | Value Type    | Value              |
 | -------- | ------------- | ------------  | ------------------ |
 | General  | Name          | String        | Set date predicate |
 | Settings | Variable type | Radio Button  | Pipeline variable  |
 | Settings | Name          | String        | datepredicate      |
 | Settings | Value         |Dynamic Content|@if(equals(pipeline().parameters.sqlenddate,null),concat('LastEdited >= ''', pipeline().parameters.sqlstartdate,''''),concat('LastEdited >= ''',pipeline().parameters.sqlstartdate,''' and LastEdited < ''',pipeline().parameters.sqlenddate,''''))    |
   
ii. Add **Copy Data** activity, drag the green arrow from the previous activity to it and configure:

 | Tab         | Configuration             | Value Type     | Value                          |
 | -------     | ------------------------- | ------------   | ------------------------------ |
 | General     | Name                      | String         | Get incremental fact data      |
 | Source      | Data store type           | Radio button   | External                       |
 | Source      | Workspace data store type | Drop down      | SQL Server (Data Source)       |
 | Source      | Data Warehouse            | Drop down      |\<choose your source connection>|
 | Source      | Use query                 | Radio button   | Query                          |
 | Source      | Query                     | Dynamic Content| select * from @{pipeline().parameters.sqlsourceschema}.@{pipeline().parameters.sqlsourcetable} where @{variables('datepredicate')}; |
 | Destination | Data store type           | Radio button   | Workspace                                          |
 | Destination | Workspace data store type | Drop down      | Lakehouse                                          |
 | Destination | Lakehouse                 | Drop down      | \<choose your Fabric lakehouse>                    |
 | Destination | Root folder               | Radio button   | Files                                              |
 | Destination | File Path (1)             | Dynamic Content| incremental/@{pipeline().parameters.sinktablename} |
 | Destination | File Path (2)             | Dynamic Content| @{pipeline().parameters.sinktablename}.parquet     |
 | Destination | File format               | Drop down      | Parquet                                            |

iii. Add **Notebook** activity, drag the green arrow from the previous activity to it and configure:

 | Tab      | Configuration               | Add New Parameter | Value Type         | Value                                                      |
 | -------- | --------------------------- | ----------------- | ------------------ | -----------------------------------------------------------|
 | General  | Settings                    |                   | String             | Merge to Data                                              |
 | Settings | Notebook                    |                   | Dropdown           | Create or Merge to Deltalake                               |
 | Settings | Advanced -> Base parameters | lakehousePath     | String             | \<enter your Lakehouse abfss path>                         |
 | Settings | Advanced -> Base parameters | tableName         | Dynamic Content    | @pipeline().parameters.sinktablename                       |
 | Settings | Advanced -> Base parameters | tableKey          | Dynamic Content    | @pipeline().parameters.tablekey                            |
 | Settings | Advanced -> Base parameters | tableKey2         | Dynamic Content    | @pipeline().parameters.tablekey2                           |
 | Settings | Advanced -> Base parameters | dateColumn        | String             | LastEdited (date column in tables showing lastedit details)|

iv. Add **Set variable** activity, drag the green arrow from the previous activity to it and configure:

 | Tab      | Configuration | Value Type      | Value                                                                           |
 | -------- | ------------- | ----------------| -----------------------------------------------------------------------------   |
 | General  | Name          | String          | Get maxdate incr                                                                |
 | Settings | Variable type | Radio Button    | Pipeline variable                                                               |
 | Settings | Name          | Dropdown        | maxdate                                                                         |
 | Settings | Value         | Dynamic Content | @split(split(activity('Merge to Data').output.result.exitValue,'\|')[0],'=')[1] |

v. Add **Set variable** activity, drag the green arrow from the previous activity to it and configure:
 | Tab      | Configuration | Value Type      | Value                                                                          |
 | -------- | ------------- | ----------------| ------------------------------------------------------------------------------ |
 | General  | Name          | String          | set rows inserted incr                                                         |
 | Settings | Variable type | Radio Button    | Pipeline variable                                                              |
 | Settings | Name          | Dropdown        | rowsinserted                                                                   |
 | Settings | Value         | Dynamic Content | @split(split(activity('Merge to Data').output.result.exitValue,'\|')[1],'=')[1]|

vi. Add **Set variable** activity, drag the green arrow from the previous activity to it and configure:
 | Tab      | Configuration | Value Type      | Value                                                                          |
 | -------- | ------------- | ----------------| ------------------------------------------------------------------------------ |
 | General  | Name          | String          | set rows updated incr                                                          |
 | Settings | Variable type | Radio Button    | Pipeline variable                                                              |
 | Settings | Name          | Dropdown        | rowsupdated                                                                    |
 | Settings | Value         | Dynamic Content | @split(split(activity('Merge to Data').output.result.exitValue,'\|')[2],'=')[1]|

vii. Add **Set variable** activity, drag the green arrow from the previous activity to it and configure:
 | Tab      | Configuration | Value Type      | Value                     |
 | -------- | ------------- | ----------------| ------------------------- |
 | General  | Name          | String          | Set pipeline endtime incr |
 | Settings | Variable type | Radio Button    | Pipeline variable         |
 | Settings | Name          | Dropdown        | pipelineendtime           |
 | Settings | Value         | Dynamic Content | @utcnow()                 |

viii. Add **Script** activity, drag the green arrow from the previous activity to it and configure:
 | Tab      | Configuration   | Value Type      | Value                                             |
 | -------- | --------------- | ------------    | ------------------------------------------------- |
 | General  | Name            | String          | Update Pipeline Run details - incremental         |
 | Settings | Data store type | Radio Button    | External                                          |
 | Settings | Connection      | Dropdown        | Connection to FabricMetdataOrchestration Database |
 | Settings | Script(1)       | Radio Button    | NonQuery                                          |
 | Settings | Script(1)       | Dynamic Content | Update dbo.PipelineOrchestrator_FabricLakehouse set batchloaddatetime = '@{pipeline().parameters.batchloaddatetime}', loadstatus = '@{activity('Get incremental fact data').output.executionDetails[0].status}', rowsread = @{activity('Get incremental fact data').output.rowsRead}, rowscopied= @{activity('Get incremental fact data').output.rowsCopied},deltalakeinserted = '@{variables('rowsinserted')}',deltalakeupdated = '@{variables('rowsupdated')}', sqlmaxdatetime = '@{variables('maxdate')}', sqlstartdate = '@{variables('maxdate')}', pipelinestarttime ='@{variables('pipelinestarttime')}', pipelineendtime = '@{variables('pipelineendtime')}' where sqlsourceschema = '@{pipeline().parameters.sqlsourceschema}' and sqlsourcetable = '@{pipeline().parameters.sqlsourcetable}'|

ix. Exit the **False activities** box of the **If condition** by clicking on  **Main canvas** in the upper left corner

You are done with this pipeline! Save your changes!

## Configure the Orchestrator Pipeline to load from Fabric Lakehouse to your Lakehouse

Now we will update the Orchestrator pipeline, named **Orchestration Pipeline**, to load data from the Fabric Lakehouse to the Lakehouse. Use the [Orchestration-Pipeline](/Accelerator_Resources/Orchestration-Pipeline.json) repo to copy the JSON code for the Orchestration Pipeline. When you are done, your pipeline should look like this: 

![Orchestration_Pipeline](images/Orchestration_Pipeline.png) 

1. Add a Set variable activity

2. Move to the **Parameters** tab and add the following parameters: 

 | Name              | Type       |
 | ----------------- | ---------- |
 | loadbronze        | String     |

3. Move to the **Variables** tab and add the following variables: 

 | Name              | Type       |
 | ----------------- | ---------- |
 | batchloaddatetime     | String |

2. Add **Set variable** activity, drag the green arrow from the previous activity to it and configure:

 | Tab      | Configuration | Value Type         | Value                   |
 | -------- | ------------- | ------------------ | ------------------------|
 | General  | Name          | String             | Set batch load datetime |
 | Settings | Variable type | Radio Button       | Pipeline variable       |
 | Settings | Name          | Dropdown           | batchloaddatetime       |
 | Settings | Value         | Dynamic Content    | @pipeline().TriggerTime |

3.  Add **Lookup** activity, drag the green arrow from the previous activity to it and configure:

 | Tab      | Configuration   | Value Type      | Value                                                                                                                    |
 | -------- | --------------- | ----------------| ------------------------------------------------------------------------------------------------------------------------ |
 | General  | Name            | String          | Get Tables to Load to Deltalake                                                                                          |
 | Settings | Data store type | Radio button    | External                                                                                                                 |
 | Settings | Connection      | Drop down       | Connection to FabricMetdataOrchestration Database                                                                        |
 | Settings | Connection Type | Drop down       | Azure SQL Database                                                                                                       |
 | Settings | Use query       | Radio button    | Query                                                                                                                    |
 | Settings | Query           | Dynamic Content | select * from dbo.PipelineOrchestrator_FabricLakehouse where skipload=0 and @{pipeline().parameters.loadbronze} = 1      |
 | Settings | First row only  | Check box       | Not Checked                                                                                                              |

. Add **For each** activity, drag the green arrow from the previous activity to it and configure:

 | Tab        | Configuration | Value Type       | Value                                                     |
 | ---------- | ------------- | -----------------| --------------------------------------------------------- |
 | General    | Name          | String           | For each table to load to deltalake                       |
 | Settings   | Items         | Dynamic Content  | @activity('Get Tables to Load to Deltalake').output.value |

4.  Click on the pencil in the **Activities** box of the **For Each** and add an **Invoke Pipeline** activity and configure as follows:

 | Tab      | Configuration      | Parameter Name    | Value Type      | Value                           |
 | -------- | ------------------ | ----------------- | ----------------| ------------------------------- |
 | General  | Name               |                   | String          | Child_Pipeline                  |
 | Settings | Invoked pipeline   |                   | Dropdown        | Load Lakehouse Table            |
 | Settings | Wait on completion |                   | Checkbox        | Checked                         |
 | Settings | Parameters         | sqlsourcetable    | Dynamic Content | @item().sqlsourcetable          |
 | Settings | Parameters         | sinktablename     | Dynamic Content | @item().sinktablename           |
 | Settings | Parameters         | sqlsourceschema   | Dynamic Content | @item().sqlsourceschema         |
 | Settings | Parameters         | batchloaddatetime | Dynamic Content | @variables('batchloaddatetime') |
 | Settings | Parameters         | sqlstartdate      | Dynamic Content | @item().sqlstartdate            |
 | Settings | Parameters         | sqlenddate        | Dynamic Content | @item().sqlenddate              |
 | Settings | Parameters         | loadtype          | Dynamic Content | @item().loadtype                |
 | Settings | Parameters         | tablekey          | Dynamic Content | @item().tablekey                |
 | Settings | Parameters         | Tablekey2         | Dynamic Content | @item().tablekey2               |

5. Exit the **Activities** box in the **For each** activity by clicking on  **Main canvas** in the upper left corner

6. Save the Orchestration Pipeline and Run it. 

When your pipeline has finished, you should now have these tables and files in your Lakehouse: 

![Tables_In_Lakehouse](Images/Tables_In_Lakehouse.png)

Your done! You have completed an End-to-End Metadata Driven Pipeline in Fabric!

## <span style='color: Blue;'>Best Practices</span>:

- Modularize Pipeline Logic: Break down the pipeline into modular components based on data sources, transformations, and loading strategies to improve maintainability and reusability.
- Version Control: Maintain version control for metadata configurations and pipeline code to track changes and facilitate collaboration among team members.
- Error Handling: Implement robust error handling mechanisms within the pipeline to handle exceptions gracefully and ensure data consistency.
- Performance Optimization: Optimize spark notebook, dataflows and SQL queries for performance by minimizing data movement, using efficient data types, and parallelizing processing where possible.
- Documentation: Document metadata definitions, pipeline configurations, and data lineage to provide clarity and context for future development and troubleshooting.


## Testing and Validation

- write the detaiks of how to check if the pipeline has run correctly. 
**Generate SQL Queries**: Dynamically populate SQL queries based on metadata definitions to extract data from source systems, load it into the lakehouse, and perform incremental updates. 
- **Testing and Validation**: Test the pipeline thoroughly with different data scenarios to ensure it operates correctly. Validate data integrity, transformation logic, and performance against expected outcomes.
- **Monitoring and Maintenance**: Set up monitoring and alerting mechanisms to track pipeline execution and identify any failures or performance issues. Regularly review and update metadata configurations as data sources or requirements change.

## Monitoring and Error Handling
- 


