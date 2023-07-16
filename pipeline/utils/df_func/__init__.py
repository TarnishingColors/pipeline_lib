from functools import reduce
from pyspark.sql.dataframe import DataFrame


def union_multiple_dfs(dfs):
    result = reduce(DataFrame.union, dfs)
    return result
