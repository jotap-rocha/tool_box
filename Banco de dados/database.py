from typing import Union
from sqlalchemy import create_engine
import pandas as pd
import pyodbc as db
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp


def insert_data_to_sql(
    data: Union[DataFrame, pd.DataFrame],
    table_name: str,
    db: str,
    usr: str,
    pwd: str,
    port: str = "",
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    server: str = "IHMTZBDBI",
    mode: str = "append"
):

    if isinstance(data, DataFrame):
        jdbc_url = f"jdbc:sqlserver://{server}:{port};databaseName={db}"

        jdbc_properties = {
            "user": usr,
            "password": pwd,
            "driver": driver
        }

        data.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=jdbc_properties)

    elif isinstance(data, pd.DataFrame):
        connection_string = f'mssql+pyodbc://{usr}:{pwd}@{server}/{db}?driver={driver}'
        engine = create_engine(connection_string)
        data.to_sql(table_name, con=engine, if_exists=mode, index=False)
        engine.dispose()


def generate_info_to_database(
    spark_session: SparkSession,
    job_name: str,
    process: str,
    layer: str,
    scope: str,
    event_type: str,
    event: str,
    project: str,
    environment: str,
    status: str,
    msg: str = None
) -> DataFrame:

    # Creating the message dataframe
    # It will be used to insert a information row about the ingestion application execution into SQL Server Table
    data = [(job_name, process, layer, scope, event_type, event, project, environment, status, msg)]
    columns = ["nome_job","processo","camada","escopo","tipo_evento","evento","projeto","ambiente","status","mensagem"]

    sdf_informacional = spark_session.createDataFrame(data, columns)

    # Add the current date column into message dataframe
    sdf_informacional = sdf_informacional.withColumn("current_timestamp", current_timestamp())

    return sdf_informacional


def select(
    query: str,
    user: str,
    pwd: str,
    server: str,
    database: str,
    driver: str = "{ODBC Driver 17 for SQL Server}"
) -> pd.DataFrame:
    
    conn = db.connect(
        Driver = driver,
        Server = server,
        Database = database,
        UID = user,
        PWD = pwd
    )
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    data = cursor.fetchall()
    
    # Colunas extraídas
    columns_description = cursor.description

    df_columns = [tuple[0] for tuple in columns_description]

    df = pd.DataFrame.from_records(data, columns=df_columns)
    
    cursor.close()
    conn.close()
    
    return df


def truncate_table(
    table: str,
    user: str,
    pwd: str,
    server: str,
    database: str,
    driver: str = "{ODBC Driver 17 for SQL Server}"
) -> bool:

    conn = db.connect(
        Driver = driver,
        Server = server,
        Database = database,
        UID = user,
        PWD = pwd
    )
    
    cursor = conn.cursor()
    cursor.execute(f"Truncate table {table};")
    
    return True
