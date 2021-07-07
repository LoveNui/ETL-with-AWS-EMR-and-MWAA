from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, round


# This file calculates the average salary within each group of all features.
# For example, average salary for each industry/degree/jobType...

def create_spark_session(app_name: str) -> SparkSession:
    """ Create a spark session.
    """
    ss = SparkSession.builder.master('local').appName(app_name).getOrCreate()
    return ss


def read_in_data(sc: SparkSession, file: str):
    """ Return a spark DataFrame of the csv file <file>.
    """
    return sc.read.csv(file, header='true', sep=',', inferSchema=False)


def join_tables(features: DataFrame, response: DataFrame, on: str) -> DataFrame:
    """ Join the <features> and <response> DataFrames by <on>.
    """
    full = features.join(response, on)
    return full


def avg_by_feature(df: DataFrame, feature: str) -> DataFrame:
    """ Find the average salary of <feature> by groups.
    """
    return df.groupby(feature) \
        .agg(round(avg('salary'), 4).alias('average_salary')) \
        .sort('average_salary', ascending=False)


def output_result(df: DataFrame, output_location: str, output_folder: str) -> None:
    """ Save the DataFrame <df> as a csv file in the location specified by
    <output_location>.
    """
    # condense all data points in one single file
    df.coalesce(1).write.csv(path=output_location + output_folder,
                             mode='append', header=True)


if __name__ == '__main__':

    # create a spark session
    spark = create_spark_session("average salary")

    #### EXTRACT ####

    # read in the train_features.csv as a spark DataFrame
    train_features_df = read_in_data(spark, 's3://airflow-salary-prediction-de/data/train_features.csv')

    # read in the train_salaries.csv as a spark DataFrame
    train_salaries_df = read_in_data(spark, 's3://airflow-salary-prediction-de/data/train_salaries.csv')

    #### TRANSFORM ####

    # join the training features with the salary
    train_df = join_tables(train_features_df, train_salaries_df, 'jobId')

    # average salary by groups for each categorical features
    # jobId is omitted since it is unique
    avg_sal_by_companyId = avg_by_feature(train_df, 'companyId')
    avg_sal_by_jobType = avg_by_feature(train_df, 'jobType')
    avg_sal_by_degree = avg_by_feature(train_df, 'degree')
    avg_sal_by_major = avg_by_feature(train_df, 'major')
    avg_sal_by_industry = avg_by_feature(train_df, 'industry')

    #### LOAD ####

    OUTPUT_LOCATION = 's3://airflow-salary-prediction-de/output/average_salary/'

    # save the above results as csv files in the bucket
    output_result(avg_sal_by_companyId, OUTPUT_LOCATION, 'companyId')
    output_result(avg_sal_by_jobType, OUTPUT_LOCATION, 'jobType')
    output_result(avg_sal_by_degree, OUTPUT_LOCATION, 'degree')
    output_result(avg_sal_by_major, OUTPUT_LOCATION, 'major')
    output_result(avg_sal_by_industry, OUTPUT_LOCATION, 'industry')

    spark.stop()