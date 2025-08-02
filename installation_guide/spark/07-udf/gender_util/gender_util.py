import re

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def parse_gender(gender):
    male_pattern = r"^m$|^male$|^man$"
    female_pattern = r"^f$|^female$|^woman$"
    if re.search(male_pattern, gender.lower()):
        return "Male"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    return "Unknown"


parse_gender_udf = udf(parse_gender, returnType=StringType())
