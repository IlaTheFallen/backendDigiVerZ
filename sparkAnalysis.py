import os
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
import base64
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import seaborn as sns
import findspark

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
findspark.init()
spark = SparkSession.builder.appName('Spark_Analysis_App').config(
    "spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()


def eda(file):
    resp = {}
    print("Starting Analysis!")
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
        # resp['Schema'] = list(df.schema)
        # resp['Schema'] = json.dumps(df.schema)
        print('Schema')
        resp['Columns'] = df.columns
        print('Columns')
        resp['Shape'] = df.count(), len(df.columns)
        print('Shape')
        # resp['Describe'] = df.describe().toPandas().to_json()
        resp['DescribeValue'] = df.describe().toPandas().values.tolist()
        resp['DescribeHeader'] = list(df.describe().toPandas())
        # resp['Describe2'] = list(df.describe())
        # resp['Describe'] = json.dumps(df.describe().toPandas())
        # resp['DescribeJSON'] = df.describe().toJSON()
        print('Describe')
        resp['Summary'] = df.summary().toPandas().to_json()
        resp['SummaryValue'] = df.summary().toPandas().values.tolist()
        resp['SummaryHeader'] = list(df.summary().toPandas())
        # resp['Summary'] = json.dumps(df.summary().toPandas())
        # resp['SummaryJSON'] = df.summary().toJSON()
        print('Summary')
        resp['Missing_Values'] = df.select(
            [count(when(isnan(c), c)).alias(c) for c in df.columns]).toPandas().to_json()
        # resp['Missing_Values'] = json.dumps(df.select(
        #     [count(when(isnan(c), c)).alias(c) for c in df.columns]).toPandas())
        # resp['Missing_ValuesJSON'] = df.select(
        #     [count(when(isnan(c), c)).alias(c) for c in df.columns]).toJSON()
        print('Missing_Values')
        resp['Null_Values'] = df.select([count(when(col(c).isNull(), c)).alias(
            c) for c in df.columns]).toPandas().to_json()
        # resp['Null_Values'] = json.dumps(df.select([count(when(col(c).isNull(), c)).alias(
        #     c) for c in df.columns]).toPandas())
        print('Null_Values')
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
        corr_plot = sns.heatmap(corr_matrix_df, cmap="YlGnBu", annot=True)
        corr_bytes_image = BytesIO(b'')
        corr_fig = corr_plot.get_figure()
        corr_fig.savefig(corr_bytes_image, format='png')
        corr_bytes_image.seek(0)
        # resp['Correlation'] = corr_matrix_df.to_json()
        resp['Correlation'] = base64.b64encode(corr_bytes_image.getvalue()).decode()
        # resp['Correlation'] = json.dumps(corr_matrix_df)
        print('Correlation')
        plt.clf()
        corr_s_plot = sns.heatmap(corr_matrix_df_s, cmap="YlGnBu", annot=True)
        corr_s_bytes_image = BytesIO(b'')
        corr_s_fig = corr_s_plot.get_figure()
        corr_s_fig.savefig(corr_s_bytes_image, format='png')
        corr_s_bytes_image.seek(0)
        # resp['SpearmanCorrelation'] = corr_matrix_df_s.to_json()
        resp['SpearmanCorrelation'] = base64.b64encode(corr_s_bytes_image.getvalue()).decode()
        # resp['Spearman-Correlation'] = json.dumps(corr_matrix_df_s)
        print('SpearmanCorrelation')
        plt.clf()
    except Exception as e:
        print(e)
        resp['error'] = str(e)
    print("Analysis ended!")
    return resp
