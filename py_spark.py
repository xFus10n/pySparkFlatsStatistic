"""
pySpark script to upload csv files in 'raw' folder, transform and analyse them
"""
import pathlib
from datetime import datetime
from termcolor import colored as c
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import when, col, lit, regexp_replace, split, count, round, avg
from pyspark.sql.types import IntegerType as Int

address_in = pathlib.Path('.').absolute() / 'files/raw'

# column and category definition
categories = ('change', 'buy', 'want_2_rent', 'rent_by_day', 'rent', 'sell')
original_columns = ('link', 'description', 'street', 'rooms', 'm2', 'floor', 'house_type', 'price')
aggregated_columns = ('split_street', 'region', 'floor_split', 'top_floor', 'com_type', 'price_refined',
                      'most_selling_floor')

# column and category assignment
change, buy, want_2_rent, rent_by_day, rent, sell = categories
link, description, street, rooms, m2, floor, house_type, price = original_columns
split_street, region, floor_split, top_floor, com_type, price_refined, top_sell_floor = aggregated_columns


def categorize(column_price: str, price_contains: str, cat_type: str, other_col: str = None) -> Column:
    """
    Spark function to set a category based on the column's 'column_price' value 'price_contains'.
    Use with another spark function 'withColumn'.
    Output value is category type 'cat_type'.
    If 'other_col' parameter is set, then the value from that column will be in the 'other' clause
    Example:
        case when null will be returned if no match found:
        data_frame.withColumn(my_column_name, categorize(look_up_column, 'look_up_value', value_to_set))
        case when other value will be returned (the values will be kept in original column) if no match found:
        data_frame.withColumn(my_column_name, categorize(look_up_column, 'look_up_value', value_to_set, other_col=my_column_name))
    :param column_price: price column for analysis
    :param price_contains: value to match
    :param cat_type: category value to set
    :param other_col: if no match, use the value from that column (avoids unintended nulls)
    :return: conditional statement wrapped in type Column
    """
    when_clause = when(col(column_price).contains(price_contains), cat_type)
    if other_col is not None:
        when_clause = when_clause.otherwise(col(other_col))
    return when_clause


def show_dataframe(dataframe, record_count=None):
    """print out dataframe"""
    dataframe.cache()
    print("Unique records: ", dataframe.count())
    for cat in categories:
        print('category: ', cat)
        if record_count is not None:
            dataframe.where(col(com_type) == cat).show(record_count)
        else:
            dataframe.where(col(com_type) == cat).show()
    dataframe.printSchema()


def get_files(dir_location: pathlib.Path) -> list[str]:
    """search files in the drop zone"""
    return [str(path) for path in dir_location.glob("./*.csv") if path.is_file()]


def read_files(spark_session: SparkSession, files: list[str]) -> DataFrame:
    """read files in the drop zone"""
    return spark_session.read.options(delimiter=';', header='True', multiline='True', escape="\"") \
        .csv(files).dropDuplicates()


def set_categories(data_frame: DataFrame) -> DataFrame:
    """
    Set categories for records, use 'com_type' column
    Categories can be found in the header, example: change, buy, want_2_rent ...
    Each category has identifier in the price which you can use to set it
    Use 'other' keyword if no category can be identified
    Hint: Use function 'categorize' to set specific value for specific condition. Of course, you are free to choose your
    own implementation.
    :param data_frame: input DataFrame
    :return: output DataFrame
    """
    pass


def clean_price(data_frame: DataFrame) -> DataFrame:
    """
    Remove any non-numeric symbols
    Use 'price_refined' column
    :param data_frame: input DataFrame
    :return: output DataFrame
    """
    pass


def set_top_floor(data_frame: DataFrame) -> DataFrame:
    """
    From 'floor' column extract upper floor of the building. Use 'top_floor' column
    Hint: you need to create additional column to store intermediate value (check aggregated columns from the top)
    Please, ensure that column 'floor' contains only floor position (not position/max floor)
    :param data_frame: input DataFrame
    :return: output DataFrame
    """
    pass


def set_region_and_street(data_frame: DataFrame) -> DataFrame:
    """
    From 'street' column extract location, use 'region' for new column name
    Hint: you need to create additional column to store intermediate value (check aggregated columns from the top)
    Please, ensure that column 'street' contains only street (not region::street)
    :param data_frame: input DataFrame
    :return: output DataFrame
    """
    pass


def commercials_by_house_type(data_frame: DataFrame) -> DataFrame:
    """
    Count number of records for each house type
    Assuming aggregated column will have 'count' name
    Replace '-' house type with 'Unspecified' value
    Sort count values descending
    :param data_frame: input DataFrame
    :return: output DataFrame
    """
    pass


def commercials_by_category(data_frame: DataFrame) -> DataFrame:
    """
    Count number of records for each commercial type
    Assuming aggregated column will have 'count' name
    Sort count values descending
    :param data_frame: input DataFrame
    :return: aggregated DataFrame
    """
    pass


def top_zones_by_commercial_count(data_frame: DataFrame) -> DataFrame:
    """
    Number of records per each zone/region, sorted by column counts
    Assuming, that aggregated column name is 'counts'. Sort by counts, descending
    :param data_frame: input DataFrame
    :return: aggregated DataFrame
    """
    pass


def average_price_in_regions_for_category(dataframe: DataFrame, category: str) -> DataFrame:
    """
    Selects the particular records, that belongs to a specific commercial **category** and group these records by
    region to calculate an average price. The value should be rounded, up to 2 digits after the decimal point. Use
    name 'price_refined' for aggregated average value. Sort descending by 'price_refined'
    :param dataframe: input DataFrame
    :param category: one of the possible column's 'comm_type' values (sell, rent, rent_by_day).
    :return: aggregated dataframe
    """
    pass


def top_floors(data_frame: DataFrame) -> DataFrame:
    """
    Selects all records, that have value in column 'top_floor' and group these records by it to calculate a number of
    counts. Use name 'count' for aggregated value. Sort descending by 'count'
    :param data_frame: input DataFrame
    :return: aggregated dataframe
    """
    pass


def count_floors_for_category(data_frame: DataFrame, category: str) -> DataFrame:
    """
    Selects the particular records, that belongs to a specific commercial **category** and group these records by floor
    to calculate number of counts for each floor. Sort descending by 'counts'
    Use name 'counts' for aggregated count value
    :param data_frame: input DataFrame
    :param category: one of the possible column's 'comm_type' values (use 'sell').
    :return: aggregated dataframe
    """
    pass


def transform(df: DataFrame) -> DataFrame:
    """ Start transformation and clean-ups"""

    # split street and city
    df_region_and_street: DataFrame = set_region_and_street(df)

    # get top floor
    df_top_floor: DataFrame = set_top_floor(df_region_and_street)

    # categorize
    df_other: DataFrame = set_categories(df_top_floor)

    # remove hidden symbols
    df_desc: DataFrame = df_other.withColumn(description, regexp_replace(col(description), "\n", ""))

    # clean price
    df_refined: DataFrame = clean_price(df_desc)

    # cast numeric
    df_numeric: DataFrame = df_refined.withColumn(price_refined, col(price_refined).cast(Int()))
    return df_numeric.cache()


def aggregate(df_numeric: DataFrame):
    """Start aggregations"""

    print(c('Number of records for each house type :', 'green'))
    commercials_by_house_type(df_numeric).show(100, truncate=False)

    print(c('Number of records for each commercial type :', 'green'))
    commercials_by_category(df_numeric).show(100, truncate=False)

    # top zones
    print(c('Number of records  sorted by regions/zones:', 'green'))
    top_zones_by_commercial_count(df_numeric).show(100, truncate=False)

    print(c('Average price for category :', 'green'), c(sell, 'yellow'))
    average_price_in_regions_for_category(df_numeric, sell).show(100, truncate=False)

    print(c('Average price for category :', 'green'), c(rent, 'yellow'))
    average_price_in_regions_for_category(df_numeric, rent).show(100, truncate=False)

    print(c('Average price for category :', 'green'), c(rent_by_day, 'yellow'))
    average_price_in_regions_for_category(df_numeric, rent_by_day).show(100, truncate=False)

    print(c('Number of selling records for each floor (Most popular floors) :', 'green'))
    count_floors_for_category(df_numeric, sell).show(100, truncate=False)

    print(c('Number of records for each top floor (Most popular building by maximum floor) :', 'green'))
    top_floors(df_numeric).show(100, truncate=False)


def main():
    # start timer
    start_time = datetime.now()

    # create spark session and read all csv files
    paths: list[str] = get_files(address_in)
    spark: SparkSession = SparkSession.builder.master("local[*]").appName("clean_data").getOrCreate()
    df: DataFrame = read_files(spark, paths)

    # clean-ups & transformations
    df_numeric: DataFrame = transform(df)

    # analysis
    aggregate(df_numeric)

    # show timer
    time_elapsed = datetime.now() - start_time
    print(c('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed), 'blue'))


if __name__ == '__main__':
    main()
