import datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.functions import udf
import holidays

def is_belgian_holiday(date: datetime.date) -> bool:
    if date is None: 
        return None
    be_holidays = holidays.BE() 
    return date in be_holidays 

def is_weekday(the_date: datetime.date):
    print(the_date)
    print(type(the_date))
    weekno = the_date.weekday()

    if weekno < 5:
        return False
    else:  # 5 Sat, 6 Sun
        return True

def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    """Adds a column indicating whether or not the attribute `colname`
    in the corresponding row is a weekend day."""
    weekday_udf_bool = udf(is_weekday, BooleanType())
    frame = frame.withColumn(new_colname, weekday_udf_bool(frame[colname]))
    return frame


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday."""
    holiday_udf_bool = udf(is_belgian_holiday, BooleanType())
    frame = frame.withColumn(new_colname, holiday_udf_bool(frame[colname]))
    return frame



def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass
