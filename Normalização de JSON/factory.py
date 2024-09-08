
import re
import factory_tools
from typing import Union, Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    explode,
    explode_outer,
    udf,
    from_json
)

from pyspark.sql.types import (
    MapType,
    StringType,
    StructType,
    ArrayType,
    LongType,
    StructField
)


class FactorySpark():

    """Helps you process data in Spark with complete and straightforward methods.
    """

    @staticmethod
    def map_json_columns(
        sdf: DataFrame,
        exclude_columns: list = []
    ) -> Tuple[List[str], List[str]]:
        """Maps JSON columns into a DataFrame, excluding specified columns.

        Args:
            sdf (DataFrame): Spark dataframe.
            exclude_columns (Tuple[List[str], List[str]], optional): List of columns that should not be checked. Defaults to [].

        Returns:
            Tuple[List[str], List[str]]: Lists of json columns that should not be checked, in this order.
        """
        json_columns = []

        for column in sdf.columns:
            if column in exclude_columns:
                continue

            sample_value = factory_tools.get_sample(sdf, column)

            if sample_value is not None:
                json_columns, exclude_columns = factory_tools.find_json(sample_value, column, exclude_columns, json_columns)

            else:
                exclude_columns.append(column)

        return json_columns, exclude_columns


    def struct_json_columns(
        self,
        sdf: DataFrame,
        non_dict_columns: Union[str, list]
    ) -> DataFrame:
        """Structure modifier for json strings, i.e. if any column has a json object declared as a
        string in its scope, this will be identified and modified to a struct or array data type.

        Args:
            sdf (DataFrame): Spark dataframe.
            non_dict_columns (Union[str, list]):  List of columns that should not be checked.

        Returns:
            DataFrame: Dataframe with changed schema (json objects that have been transformed).
        """

        # Identificar colunas que contêm JSON strings ou números
        json_columns, non_dict_columns = FactorySpark.map_json_columns(sdf, non_dict_columns)

        # Inferir esquema e converter JSON strings para tipos estruturados
        for column in json_columns:
            sample_json_str = sdf.select(column).dropna().first()[0]
            schema = factory_tools.infer_schema_from_json(sample_json_str)
            sdf = sdf.withColumn(column, from_json(col(column), schema))

        return sdf


    def explode_structed_columns(
        self,
        sdf: DataFrame
    ) -> DataFrame:
        """Explode columns declared as struct or array data types.

        Args:
            sdf (Dataframe): Spark dataframe.

        Returns:
            DataFrame: Dataframe with a modified schema, i.e. with dictionaries or lists that have become columns and rows.
        """

        struct_columns = [field.name for field in sdf.schema.fields if isinstance(field.dataType, (ArrayType, MapType, StructType))]

        for column in struct_columns:

            # Verificar se a coluna existe no DataFrame
            if column in sdf.columns:
                # Verificação de ArrayType
                if isinstance(sdf.schema[column].dataType, ArrayType):
                    exploded_column_name = f"{column}_exploded"
                    sdf = sdf.selectExpr("*", f"explode_outer({column}) as {exploded_column_name}")

                # Verificação de MapType
                elif isinstance(sdf.schema[column].dataType, MapType):
                    # Explodir o mapa em uma coluna de chave-valor
                    exploded_sdf = sdf.withColumn(column, explode_outer(col(column)))

                    # Renomear as colunas resultantes do mapa
                    map_keys = sdf.select(explode_outer(col(column)).alias("key","value")).selectExpr("key").distinct().collect()

                    for key_row in map_keys:
                        key_name = key_row["key"]
                        sdf = exploded_sdf.withColumn(f"{column}_{key_name}_exploded", col(f"{column}[\"{key_name}\"]"))

                    # Drop da coluna original do mapa
                    sdf = sdf.drop(column)

                # Verificação de StructType
                elif isinstance(sdf.schema[column].dataType, StructType):
                    for field in sdf.schema[column].dataType.fields:
                        old_name = str(field.name)
                        if old_name == " ambiente":
                            new_name = old_name.replace(" ", "primeiro_")
                        else:
                            new_name = old_name.replace(".", "").replace("_", "").replace(" ", "").replace("í", "i")
                        sdf = sdf.selectExpr("*", f"{column}.`{field.name}` as {column}_{new_name}") #! INFORMAR a não colocar . como nome de coluna

                sdf = sdf.drop(column)

        return sdf


    def analyze_columns_spark(sdf: DataFrame, spark: SparkSession = None) -> DataFrame:
        import pyspark.sql.functions as F

        if spark is None:
            raise ValueError("O parâmetro 'spark' não pode ser None.")

        # Obter o nome das colunas
        columns = sdf.columns

        results = []
        for col in columns:
            col_type = sdf.schema[col].dataType

            # Obter o valor máximo e o comprimento máximo da string
            if isinstance(col_type, LongType):
                # Para LongType, o MaxString deve ser o maior valor convertido para string
                max_value = sdf.agg(F.max(F.col(col)).alias("max_value")).collect()[0]["max_value"]
                max_string = str(max_value) if max_value is not None else None
            elif isinstance(col_type, StringType):
                # Para StringType, o MaxString deve ser a string máxima identificada
                max_string = sdf.agg(F.max(F.col(col)).alias("max_string")).collect()[0]["max_string"]
                max_value = len(max_string) if max_string else None
            else:
                # Para outros tipos, definimos como None
                max_value = None
                max_string = None

            results.append({
                'Tabela': 'cloud_database',  # Alterar conforme necessário
                'Coluna': col,
                'TipagemColuna': str(col_type),
                'MaxString': max_string,
                'MaxValue': max_value
            })

        # Definir o schema para o DataFrame final
        schema = StructType([
            StructField('Tabela', StringType(), True),
            StructField('Coluna', StringType(), True),
            StructField('TipagemColuna', StringType(), True),
            StructField('MaxString', StringType(), True),
            StructField('MaxValue', LongType(), True)  # Garantindo que MaxValue seja LongType
        ])

        # Criar um DataFrame Spark com os resultados
        result_sdf = spark.createDataFrame(results, schema=schema)

        return result_sdf


    def create_id(
        self,
        sdf: DataFrame,
        name_id: str = "id"
    ) -> DataFrame:
        """Creates an identifier (id) for the table.

        Args:
            sdf (DataFrame): _description_
            name_id (str, optional): Name to be given to the identifier column. Defaults to "id".

        Returns:
            DataFrame: Dataframe with an id column.
        """
        sdf = sdf.withColumn(name_id, monotonically_increasing_id())

        # Reordenar as colunas para colocar 'id' como a primeira coluna
        columns_ordered = [name_id] + [column for column in sdf.columns if column != name_id]
        sdf = sdf.select(columns_ordered)

        return sdf


    def unify_column_names(
        self,
        sdf: DataFrame,
        target_character: str =" ",
        new_character: str = "_"
    ) -> DataFrame:
        """Join the characters of the column name with an underline if there is a space between them.
        We recommend doing this to avoid problems with Spark processing, which is sensitive to spaces.

        Args:
            sdf (Dataframe): Spark dataframe.
            target_column (str, optional): Old character.
            new_character (str, optional): New character.

        Returns:
            DataFrame: Dataframe, with columns that had a space in the name, renamed.
        """
        renamed_columns = {col_name: col_name.replace(target_character, new_character) for col_name in sdf.columns}
        for old_name, new_name in renamed_columns.items():
            sdf = sdf.withColumnRenamed(old_name, new_name)

        return sdf


    def load(
        self,
        sdf: DataFrame,
        path: str,
        mode: str,
        format: str
    ) -> None:
        
        sdf.write\
            .mode(mode) \
            .format(format) \
            .save(path)

    #             logger.info("---------#####--------")
    #             logger.info('Escrita na camada silver no formato delta feita com sucesso!')
    #             logger.info("---------#####--------")

        return None


    def break_multiple_lists(
        self,
        sdf: DataFrame,
        target_columns: Union[str, list] = ["Properties_Headers_Response","Properties_Headers_Request"]
    ) -> DataFrame:
        """Breaks down lists that simulate a dictionary, turning the first item in the list into a column
        and the second item into a line. It is advisable to check whether this method is necessary, as its
        performance is very degraded.

        Args:
            sdf (DataFrame): Spark dataframe.
            target_columns (Union[str, list], optional): Columns with dictionary-like lists. Defaults to ["Properties_Headers_Response","Properties_Headers_Request"].

        Returns:
            DataFrame: Dataframe handled.
        """

        # Função para converter listas de pares chave-valor em um dicionário
        def extract_key_value_pairs(rows):
            result = {}

            # Verificar se a entrada é válida
            if rows is None:
                return result
            
            list_str = rows.split(",")
            
            key = None
            value = None
            
            for index, string in enumerate(list_str):
                if string != ' ]' and index % 2 == 0:
                    # Criando uma expressão regular que corresponde a "[" ou " ["
                    regular_expression = r" ?\["
                    
                    # Chave encontrada
                    key = re.sub(regular_expression, "", string.strip())
                
                elif string != ' ]' and index % 2 > 0:
                    # Criando uma expressão regular que corresponde a " " ou "]"
                    regular_expression = r"[ ]|\]"

                    # Valor encontrado
                    value = re.sub(regular_expression, "", string.strip())
                    
                    result[key] = value
                    key = None  # Reset key
                else:
                    value = None
                    
                    if key is not None:
                        result[key] = value
                        key = None  # Reset key
            
            return result

        # Definir a UDF
        extract_key_value_pairs_udf = udf(extract_key_value_pairs, MapType(StringType(), StringType()))

        for column in target_columns:

            if column in sdf.columns:
                # Aplicar a UDF ao DataFrame
                sdf = sdf.withColumn(f"{column}_Map", extract_key_value_pairs_udf(col(column)))

                # Obter todas as chaves possíveis
                keys = sdf.select(explode(col(f"{column}_Map")).alias("key", "value")).select("key").distinct().rdd.flatMap(lambda x: x).collect()

                # Criar colunas individuais para cada chave
                for key in keys:
                    if "-" in key:
                        key = key.replace("-", "")
                    sdf = sdf.withColumn(f"{column}_{key}", col(f"{column}_Map").getItem(key))

                # Remover a coluna intermediária
                sdf = sdf.drop(column)

                # Remover a coluna intermediária
                sdf = sdf.drop(f"{column}_Map")

        return sdf
