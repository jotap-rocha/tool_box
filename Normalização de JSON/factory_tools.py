
import json
from pyspark.sql import DataFrame

from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    ArrayType
)


def infer_schema(
    json_obj: json
):
    """Transforms the json object from the dictionary type to the struct data type and from the list type to the array data type.

    Args:
        json_obj (json): json object (dict or list) that will be transformed into a struct or array data type.

    Returns:
        StructType: Returns a schema compatible with the structure received.
    """
    if isinstance(json_obj, dict):
        return StructType([StructField(key, infer_schema(value), True) for key, value in json_obj.items()])

    elif isinstance(json_obj, list):
        if json_obj:
            return ArrayType(infer_schema(json_obj[0]))

        else:
            return ArrayType(StringType())

    else:
        return StringType()


def infer_schema_from_json(
    json_str: str
):
    """Automatically infer a structure for the json in order to transform it from a string data type into a struct data type.

    Args:
        json_str (str): Sample dataframe.

    Returns:
        infer_schema(json_obj): Struct data type.
    """
    try:
        json_obj = json.loads(json_str)
        return infer_schema(json_obj)
    
    except json.JSONDecodeError:
        return StringType()


# def map_struct_columns(schema):
#     struct_columns = []
    
#     for field in schema.fields:
#         if isinstance(field.dataType, (ArrayType, StructType, MapType)):
#             struct_columns.append(field.name)
    
#     return struct_columns


def is_float(value: str):
    """Checks for the presence of the float data type.

    Args:
        value (str): sample dataframe.

    Returns:
        bool: If true, then there's a float in the column, otherwise it's not a float.
    """
    
    try:
        # Verificar se a string é um número
        float(value)
        return True

    except ValueError:
        return False


def is_boolean(value: str):
    """Checks for the presence of the bool data type.

    Args:
        value (str): sample dataframe.

    Returns:
        bool: If true, then there's a bool in the column, otherwise it's not a bool.
    """

    return value.lower() in ["true", "false"]


def is_json(value: str):
    """Checks for the presence of the json.

    Args:
        value (str): sample dataframe.

    Returns:
        bool: If true, then there's a json in the column, otherwise it's not a json.
    """
    try:
        json.loads(value)
        return True
    
    except json.JSONDecodeError:
        return False


def get_sample(
    sdf: DataFrame, 
    column: str
):
    """Captures a non-null sample from the column

    Args:
        sdf (DataFrame): Dataframe spark.
        column (str): Target column.

    Returns:
        non_null_sample[0]: non-null sample base.
    """
    non_null_sample = sdf.select(column).dropna().first()
    return non_null_sample[0] if non_null_sample else None


def find_json(
    sample_value: str,
    column: str,
    exclude_columns: list,
    json_columns: list
):
    """Identifier of json structure hidden in the table.

    Args:
        sample_value (str): Sample dataframe.
        column (str): Target column.
        exclude_columns (list): List of columns that don't need to be checked.
        json_columns (list): List of columns that are json.
    
    Returns:
        json_columns: List containing the name of the columns that have json.
        exclude_columns: List containing the names of the columns that don't have json.
    """

    if isinstance(sample_value, str):
        if is_float(sample_value):
            exclude_columns.append(column)
        elif is_boolean(sample_value):
            exclude_columns.append(column)
        elif is_json(sample_value):
            json_columns.append(column)
        else:
            exclude_columns.append(column)
    else:
        exclude_columns.append(column)
    
    return json_columns, exclude_columns
