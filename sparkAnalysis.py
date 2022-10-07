from email import header
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, regexp_replace, substring, to_timestamp, to_date, round, isnan, count
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import pyspark.pandas as ps
import json
import pandas as pd
from io import BytesIO, StringIO
import findspark

findspark.init()
spark = SparkSession.builder.appName('Spark_Analysis_App').config(
    "spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()


def eda(file):
    resp = {}
    try:
        if file.content_type == 'text/csv':
            file.save('temp.csv')
            df = spark.read.csv('temp.csv', header=True, inferSchema=True)
        else:
            file.save('temp.xlsx')
            data = ps.read_excel('temp.xlsx')
            df = data.to_spark()
            # data = pd.read_excel(file.read())
            # print(data)
            # data.to_csv('temp.csv', sep='\t', encoding='utf-8')
            # df = spark.createDataFrame(data)
            # csv = str(file.read())[2:-1]
            # csv = csv.replace('\\t',',').replace('\\n','\n')
            # file1 = open('temp.csv','w')
        # print(file.read())
        # s = str(file.read(), 'utf-8')
        # print(s)
        # data = ' '.join(s)
        # data = s.replace(',',' ')
        # data = [x.strip().split(' ') for x in data.split('\n')]
        # data = StringIO(s)
        # print(data)
        # rdd = spark.sparkContext.parallelize(data)
        # print("RDD done.")
        # df = spark.createDataFrame(pd.read_csv(BytesIO(file.read())))
        # print("df done.")
        # resp['content'] = df
        resp['Schema'] = df.schema.json()
        resp['Columns'] = df.columns
        resp['Shape'] = df.count(), len(df.columns)
        resp['Describe'] = df.describe().toPandas().to_json()
        resp['Summary'] = df.summary().toPandas().to_json()
        resp['Missing_Values'] = df.select(
            [count(when(isnan(c), c)).alias(c) for c in df.columns]).toPandas().to_json()
        resp['Null_Values'] = df.select([count(when(col(c).isNull(), c)).alias(
            c) for c in df.columns]).toPandas().to_json()
        columns_not_string = [
            column.name for column in df.schema if column.dataType != StringType()]
        vector_col = "corr_vars"
        assembler = VectorAssembler(
            inputCols=columns_not_string, outputCol=vector_col)
        df_vector = assembler.transform(df).select(vector_col)
        corr_matrix = Correlation.corr(df_vector, vector_col).collect()[
            0][0].toArray().tolist()
        corr_matrix_s = Correlation.corr(df_vector, vector_col, "spearman").collect()[
            0][0].toArray().tolist()
        corr_matrix_df = pd.DataFrame(
            data=corr_matrix, columns=columns_not_string, index=columns_not_string)
        corr_matrix_df_s = pd.DataFrame(
            data=corr_matrix_s, columns=columns_not_string, index=columns_not_string)
        resp['Correlation'] = corr_matrix_df.to_json()
        resp['Spearman-Correlation'] = corr_matrix_df_s.to_json()
    except Exception as e:
        print(e)
        resp['error'] = str(e)
    return resp
