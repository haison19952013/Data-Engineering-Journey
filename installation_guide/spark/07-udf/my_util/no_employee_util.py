import re

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StructType, StructField


def parse_employees(val):
    if val is None:
        return (None, None)
    if "More than" in val:
        num = int(re.search(r"\d+", val).group())
        return (num + 1, None)  # (lower bound, open upper bound)
    elif "-" in val:
        lo, hi = val.split("-")
        return (int(lo), int(hi))
    else:
        try:
            return (int(val), int(val))
        except:
            return (None, None)

schema = StructType([
    StructField("low", IntegerType(), True),
    StructField("high", IntegerType(), True)
])
parse_employees_udf = udf(parse_employees, returnType=schema)
