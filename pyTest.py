import logging
import pytest
import py_spark as main
from pyspark_test import assert_pyspark_df_equal
# from pyspark import SparkConf
# from pyspark import SparkContext
from pyspark.sql import SparkSession


# source: https://stackoverflow.com/questions/40975360/testing-spark-with-pytest-cannot-run-spark-in-local-mode
def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


# @pytest.fixture(scope="session")
# def spark_context(request):
#     conf = (SparkConf().setMaster("local[*]").setAppName("pytest-pyspark-local-testing"))
#     request.addfinalizer(lambda: sc.stop())
#     sc = SparkContext(conf=conf)
#     quiet_py4j()
#     return sc


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = SparkSession.builder.getOrCreate()
    request.addfinalizer(lambda: spark_session.stop())
    quiet_py4j()
    return spark_session


'''EXAMPLES:'''


# @pytest.fixture(scope="session")
# def hive_context(spark_context):
#     return HiveContext(spark_context)
#
#
# @pytest.fixture(scope="session")
# def streaming_context(spark_context):
#     return StreamingContext(spark_context, 1)


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


def get_data_frame(spark_session):
    columns = main.original_columns
    data = [('link1', 'desc1', 'centrs::Valdemāra 106', '3', '72', '1/5', 'Staļina', '105,000  €'),
            ('link2', 'desc2', 'Zolitūde::Lejiņa 18', '3', '74', '10/10', '119.', '96,000  €'),
            ('link3', 'desc3', 'Jugla::Murjāņu 52', '2', '44', '1/5', 'Hrušč.', '44,000  €')]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_region_extract(spark_session):
    columns = (main.region, main.street)
    data = [('centrs', 'Valdemāra 106'),
            ('Zolitūde', 'Lejiņa 18'),
            ('Jugla', 'Murjāņu 52')]
    return spark_session.createDataFrame(data).toDF(*columns)


def get_output_for_floor_extract(spark_session):
    columns = (main.floor, main.top_floor)
    data = [('1', '5'),
            ('10', '10'),
            ('1', '5')]
    return spark_session.createDataFrame(data).toDF(*columns)
