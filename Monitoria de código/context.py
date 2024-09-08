from pyspark.sql import SparkSession
from logging import Logger

class JobContext:
    def __init__(
        self, 
        logger: Logger,
        job_name: str,
        process: str,
        project: str,
        environment: str,
        spark: SparkSession = None
        # Opcional: Adicione mais parâmetros e crie as variáveis abaixo
    ):
        self.logger = logger
        self.spark = spark
        self.job_name = job_name
        self.process = process
        self.project = project
        self.environment = environment
