import sys
import traceback
from datetime import datetime
from logging import Logger
from pyspark.sql import SparkSession


def log_and_exit(logger: Logger, job_name: str, process: str, project: str,
                environment: str,error_function: str, message: str, error: str,
                spark: SparkSession = None):

    where_error_ocurred = "".join(
        [f'{frame.filename}: line {frame.lineno} in {frame.name}()\n' for frame in traceback.extract_tb(error.__traceback__)]
    )

    handle_error(logger=logger, job_name=job_name, process=process, project=project,
                environment=environment, error_function=error_function,
                message=message, context=where_error_ocurred, error=str(error))

    if spark:
        spark.stop()
    sys.exit()


def handle_error(logger: Logger, job_name:str, process: str, project: str, environment: str,
                error_function: str, message: str, context: str = "", error: str = "Not informed"):
    if error_function == "":
        logger.error(f"\n! ! ! !\n\n{context}\n\nErro durante a execução do job: {error}\n\n! ! ! !")
    else:
        logger.error(f"\n! ! ! !\n\n{context}\n\nErro durante a execução do job. Passo de execução '{error_function}': {error}\n\n! ! ! !")

    status = "FAILED"
    try:
        store_log_database(job_name=job_name, process=process, project=project, environment=environment,
                        status=status, error_function=error_function, message=message,
                        user=user, password=password, table=log_table, db=log_database,
                        path_json=path_json)
    
    except Exception as e:
        logger.error(f"\n! ! ! !\n\nErro durante o armazenamento do log no banco: {e}\n\n! ! ! !")


def handle_warning(message:str, logger: Logger, job_name: str,
                    process: str, project: str, environment:str):
    
    logger.warning(f"\n! ! ! !\n\nATENÇÃO {message}\n\n! ! ! !")
    
    status = "WARNING"
    
    store_log_database(job_name=job_name, process=process, project=project,
                        environment=environment, status=status, message=message)


def store_log_database(job_name:str, user: str, password: str, table: str, db: str,
                        process: str, project: str, environment: str, status: str,
                        path_json: str, message: str, error_function: str = None):
    
    timestamp = datetime.now()
    
    data = {
        "nome_job": [job_name],
        "processo": [process],
        "projeto": [project],
        "ambiente": [environment],
        "status": [status],
        "funcao_problema": [error_function],
        "path_json": [path_json],
        "mensagem": [message],
        "current_timestamp": [timestamp]
    }

    df = pd.DataFrame(data)

    # Ingere no SQL Server todas as informações que montei
    database.insert_data_to_sql(
        data = df,
        usr = user,
        pwd = password,
        table_name = table,
        db = db,
        driver = "ODBC Driver 17 for SQL Server"
    )