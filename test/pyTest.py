import logging
import pytest
import py_spark as main
from pyspark_test import assert_pyspark_df_equal
from pyspark.sql import SparkSession


# source: https://stackoverflow.com/questions/40975360/testing-spark-with-pytest-cannot-run-spark-in-local-mode
def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = SparkSession.builder.getOrCreate()
    request.addfinalizer(lambda: spark_session.stop())
    quiet_py4j()
    return spark_session


def test_top_floor_extract(spark_session):
    # arrange
    test_df = get_data_frame(spark_session)
    expected_df = get_output_for_floor_extract(spark_session)

    # act
    output_df = main.set_top_floor(test_df)
    actual_df = output_df.select(main.floor, main.top_floor)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def test_region_extract(spark_session):
    # arrange
    test_df = get_data_frame(spark_session)
    expected_df = get_output_for_region_extract(spark_session)

    # act
    output_df = main.set_region_and_street(test_df)
    actual_df = output_df.select(main.region, main.street)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def test_number_of_records_by_region(spark_session):
    # arrange
    test_df = get_data_frame(spark_session)
    expected_df = get_output_for_top_zones(spark_session)

    # act
    output_df = main.set_region_and_street(test_df)
    actual_df = main.top_zones_by_commercial_count(output_df).sort('counts', main.region, ascending=False)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def test_categorization(spark_session):
    # arrange
    test_df = get_data_frame(spark_session)
    expected_df = get_output_for_categorization(spark_session)

    # act
    output_df = main.set_categories(test_df)
    actual_df = output_df.select(main.com_type)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def test_clean_price(spark_session):
    # arrange
    test_df = get_data_frame(spark_session)
    expected_df = get_output_for_price_refinement(spark_session)

    # act
    output_df = main.clean_price(test_df)
    actual_df = output_df.select(main.price_refined)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def test_agg_house_types_count(spark_session):
    # arrange
    test_df = get_data_frame(spark_session)
    expected_df = get_output_for_house_type(spark_session)

    # act
    output_df = main.commercials_by_house_type(test_df)

    # assert
    assert_pyspark_df_equal(expected_df, output_df)


def test_agg_category_types_count(spark_session):
    # arrange
    test_df = get_data_frame(spark_session)
    expected_df = get_output_for_com_type(spark_session)

    # act
    categories_df = main.set_categories(test_df)
    output_df = main.commercials_by_category(categories_df).sort(main.com_type)

    # assert
    assert_pyspark_df_equal(expected_df, output_df)


def test_average_price_for_region(spark_session):
    # assign
    test_df = get_data_frame_2(spark_session)
    expected_df = get_output_for_average_price_in_sell_category(spark_session)

    # act
    categories_df = main.set_categories(test_df)
    numeric_df = main.clean_price(categories_df)
    region_df = main.set_region_and_street(numeric_df)
    actual_df = main.average_price_in_regions_for_category(region_df, main.sell)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def test_most_popular_floor(spark_session):
    # assign
    test_df = get_data_frame_2(spark_session)
    expected_df = get_output_for_most_popular_floor(spark_session)

    # act
    floor_df = main.set_top_floor(test_df)
    categories_df = main.set_categories(floor_df)
    actual_df = main.count_floors_for_category(categories_df, main.sell)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def test_count_top_floors(spark_session):
    # assign
    test_df = get_data_frame_2(spark_session)
    expected_df = get_output_for_top_floor(spark_session)

    # act
    floor_df = main.set_top_floor(test_df)
    actual_df = main.top_floors(floor_df).sort('count', main.top_floor, ascending=False)

    # assert
    assert_pyspark_df_equal(expected_df, actual_df)


def get_data_frame(spark_session):
    columns = main.original_columns
    data = [('link1', 'desc1', 'centrs::Valdemāra 106', '3', '72', '1/5', '-', '105,000  €'),
            ('link2', 'desc2', 'centrs::Lejiņa 18', '3', '74', '10/10', '119.', 'vēlosīret'),
            ('link3', 'desc3', 'Jugla::Murjāņu 52', '2', '44', '1/5', 'Hrušč.', '250  €/mēn.'),
            ('link4', 'desc4', 'centrs::Matīsa 41', '1', '20', '1/2', 'Renov.', '30  €/dienā'),
            ('link5', 'desc5', 'Āgenskalns::', '3', '65', '-', 'Renov.', 'pērku'),
            ('link5', 'desc5', 'Pļavnieki::', '2', '-', '-', '119.', '175,000  €maiņai')]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_data_frame_2(spark_session):
    columns = main.original_columns
    data = [('link1', 'desc1', 'centrs::', '3', '72', '2/5', '-', '105,003  €'),
            ('link1', 'desc1', 'centrs::', '3', '72', '1/5', '-', '99,007  €'),
            ('link2', 'desc2', 'centrs::', '3', None, '10/10', '119.', 'vēlosīret'),
            ('link3', 'desc3', 'Jugla::', '2', '44', '3/5', 'Hrušč.', '55,000  €'),
            ('link4', 'desc4', 'centrs::', '1', '20', '1/2', 'Renov.', '115,000  €'),
            ('link5', 'desc5', 'Jugla::', '3', '65', '1/3', 'Renov.', '60,007  €'),
            ('link5', 'desc5', 'Jugla::', '3', '65', '3/3', 'Renov.', '65,004  €'),
            ('link5', 'desc5', 'Pļavnieki::', '2', '45', '-', '119.', 'maiņai')]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_region_extract(spark_session):
    columns = main.region, main.street
    data = [('centrs', 'Valdemāra 106'),
            ('centrs', 'Lejiņa 18'),
            ('Jugla', 'Murjāņu 52'),
            ('centrs', 'Matīsa 41'),
            ('Āgenskalns', ''),
            ('Pļavnieki', '')]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_average_price_in_sell_category(spark_session):
    columns = main.region, main.price_refined
    data = [('centrs', 106336.67),
            ('Jugla', 60003.67)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_top_zones(spark_session):
    columns = main.region, 'counts'
    data = [('centrs', 3),
            ('Āgenskalns', 1),
            ('Pļavnieki', 1),
            ('Jugla', 1)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_floor_extract(spark_session):
    columns = main.floor, main.top_floor
    data = [('1', '5'), ('10', '10'), ('1', '5'), ('1', '2'), ('-', None), ('-', None)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_house_type(spark_session):
    columns = main.house_type, 'count'
    data = [('119.', 2), ('Renov.', 2), ('Unspecified', 1), ('Hrušč.', 1)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_categorization(spark_session):
    columns = main.com_type,
    data = [(main.sell,), (main.want_2_rent,), (main.rent,), (main.rent_by_day,), (main.buy,), (main.change,)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_price_refinement(spark_session):
    columns = main.price_refined,
    data = [('105000',), ('',), ('250',), ('30',), ('',), ('175000',)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_most_popular_floor(spark_session):
    columns = main.floor, 'counts'
    data = [("1", 3), ("3", 2), ("2", 1)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_top_floor(spark_session):
    columns = main.top_floor, 'count'
    data = [("5", 3), ("3", 2), ("2", 1), ("10", 1)]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_com_type(spark_session):
    columns = main.com_type, 'count'
    data = [(main.buy, 1), (main.change, 1), (main.rent, 1), (main.rent_by_day, 1), (main.sell, 1),
            (main.want_2_rent, 1)]
    return spark_session.createDataFrame(data).toDF(*columns)
