import pathlib
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType as Int

address_in = pathlib.Path('.').absolute() / 'files/raw'
address_out = pathlib.Path('.').absolute() / 'files/clean'

# column and category definition
categories = ('change', 'buy', 'want_2_rent', 'rent_by_day', 'rent', 'sell')
original_columns = ('link', 'description', 'street', 'rooms', 'm2', 'floor', 'house_type', 'price')
aggregated_columns = ('split_street', 'region', 'floor_split', 'top_floor', 'com_type', 'price_refined',
                      'most_selling_floor')

# column and category assignment
change, buy, want_2_rent, rent_by_day, rent, sell = categories
link, description, street, rooms, m2, floor, house_type, price = original_columns
split_street, region, floor_split, top_floor, com_type, price_refined, top_sell_floor = aggregated_columns


def categorize(column_price, price_contains, cat_type, other_col=None):
    when_clause = func.when(func.col(column_price).contains(price_contains), cat_type)
    if other_col is not None:
        when_clause = when_clause.otherwise(func.col(other_col))
    return when_clause


def show_dataframe(dataframe, record_count=None):
    dataframe.cache()
    print("Unique records: ", dataframe.count())
    for cat in categories:
        print('category: ', cat)
        if record_count is not None:
            dataframe.where(func.col(com_type) == cat).show(record_count)
        else:
            dataframe.where(func.col(com_type) == cat).show()
    dataframe.printSchema()


def get_files(dir_location): return [str(path) for path in dir_location.glob("./*.csv") if path.is_file()]


def read_files(spark_session, files):
    return spark_session.read.options(delimiter=';', header='True', multiline='True', escape="\"") \
        .csv(files).dropDuplicates()


def set_categories(data_frame):
    df_sell = data_frame.withColumn(com_type, categorize(price, '€', sell))
    df_rent = df_sell.withColumn(com_type, categorize(price, '€/mēn', rent, other_col=com_type))
    df_rent_day = df_rent.withColumn(com_type, categorize(price, '€/dienā', rent_by_day, other_col=com_type))
    df_want = df_rent_day.withColumn(com_type, categorize(price, 'vēlosīret', want_2_rent, other_col=com_type))
    df_buy = df_want.withColumn(com_type, categorize(price, 'pērku', buy, other_col=com_type))
    df_chng = df_buy.withColumn(com_type, categorize(price, 'maiņai', change, other_col=com_type))
    return df_chng.withColumn(com_type,
                              func.when(df_chng.com_type.isNull(), func.lit('other')).otherwise(df_chng.com_type))


def clean_price(data_frame):
    df_ref1 = data_frame.withColumn(price_refined, func.regexp_replace(func.col(price), "€", ""))
    df_ref2 = df_ref1.withColumn(price_refined, func.regexp_replace(func.col(price_refined), "/mēn.", ""))
    df_ref3 = df_ref2.withColumn(price_refined, func.regexp_replace(func.col(price_refined), "/dienā", ""))
    df_ref4 = df_ref3.withColumn(price_refined, func.regexp_replace(func.col(price_refined), "maiņai", ""))
    df_ref5 = df_ref4.withColumn(price_refined, func.regexp_replace(func.col(price_refined), "pērku", ""))
    df_ref6 = df_ref5.withColumn(price_refined, func.regexp_replace(func.col(price_refined), "vēlosīret", ""))
    return df_ref6.withColumn(price_refined, func.regexp_replace(func.col(price_refined), ",", ""))


def set_top_floor(data_frame):
    df_top_floor_split = data_frame.withColumn(floor_split, func.split(data_frame.floor, "/"))
    return df_top_floor_split.withColumn(top_floor, df_top_floor_split.floor_split.getItem(1)) \
        .withColumn(floor, df_top_floor_split.floor_split.getItem(0))


def set_region_and_street(data_frame):
    df_split = data_frame.withColumn(split_street, func.split(data_frame.street, "::"))
    return df_split.withColumn(region, df_split.split_street.getItem(0)) \
        .withColumn(street, df_split.split_street.getItem(1))


def commercials_by_house_type(data_frame):
    return data_frame.select(house_type) \
        .withColumn(house_type, func.regexp_replace(func.col(house_type), '-', 'Unspecified')) \
        .groupby(house_type) \
        .count() \
        .sort('count', ascending=False)


def commercials_by_category(data_frame):
    return data_frame.select(com_type) \
        .groupby(com_type) \
        .count() \
        .sort('count', ascending=False)


def top_zones_by_commercial_count(data_frame):
    return data_frame.select(region).groupby(region).agg(func.count(region).alias('counts')) \
        .sort('counts', ascending=False)


def average_price_by_category(dataframe, category):
    return dataframe.filter(func.col(com_type).eqNullSafe(category)).groupby(region) \
        .agg(func.round(func.avg(price_refined), 2).alias(price_refined)).sort(price_refined, ascending=False)


def top_floors(data_frame):
    return data_frame.dropna().select(top_floor).groupby(top_floor).count().sort('count', ascending=False)


def count_selling_floors(data_frame, category):
    return data_frame.select(com_type, floor).filter(func.col(com_type).eqNullSafe(category)).groupby(floor)\
        .agg(func.count(floor).alias('sell_counts')).sort('sell_counts', ascending=False)


def main():
    # create spark session and read all csv files
    paths = get_files(address_in)
    spark = SparkSession.builder.master("local[*]").appName("clean_data").getOrCreate()
    df = read_files(spark, paths)

    # split street and city
    df_region_and_street = set_region_and_street(df)

    # get top floor
    df_top_floor = set_top_floor(df_region_and_street)

    # categorize
    df_other = set_categories(df_top_floor)

    # remove hidden symbols
    df_desc = df_other.withColumn(description, func.regexp_replace(func.col(description), "\n", ""))

    # clean price
    df_refined = clean_price(df_desc)

    # cast numeric
    df_numeric = df_refined.withColumn(price_refined, func.col(price_refined).cast(Int()))
    df_numeric.cache()

    # commercial by house type
    commercials_by_house_type(df_numeric).show()

    # commercials by commercial type
    commercials_by_category(df_numeric).show()

    # top zones
    top_zones_by_commercial_count(df_numeric).show()

    # average prices (sell, rent, rent by day)
    average_price_by_category(df_numeric, sell).show()
    average_price_by_category(df_numeric, rent).show()
    average_price_by_category(df_numeric, rent_by_day).show()

    # top-selling floor
    count_selling_floors(df_numeric, sell).show()
    top_floors(df_numeric).show()


if __name__ == '__main__':
    main()
