# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d9cb1a7a-1b03-4c6c-ba0e-26bc822e46b5",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "851e1ae1-7097-4772-b330-584465b56abf",
# META       "known_lakehouses": [
# META         {
# META           "id": "d9cb1a7a-1b03-4c6c-ba0e-26bc822e46b5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Get all the files under the ADLS folder and create a list of file paths
file_list = mssparkutils.fs.ls("abfss://medallion_architecture@onelake.dfs.fabric.microsoft.com/bronze.Lakehouse/Files/raw")
#Read each file and create a Dataframe
for file_path in file_list:
    print(file_path)
    df = spark.read.format("csv").options(inferSchema="true", header="true").load(path=f"{file_path.path}*")
    # You can process the Dataframe or register it as a table here
    # to create a temporary table:
    df.createOrReplaceTempView(file_path.name.removesuffix('.csv'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sql = spark.sql("SELECT * FROM salesltaddress LIMIT 100")
display(df_sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
