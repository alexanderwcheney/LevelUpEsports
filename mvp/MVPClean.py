if __name__ == "__main__":
    print('start program')
    spark = SparkSession\
        .builder\
        .appName("TransformEsportMatchData")\
        .getOrCreate()

    df = spark.read.json('s3a_bucket/json_file')
    df = df.filter(df.winner.isNotNull()).withColumn('winning_team', df.winner['name'])
    df2 = df.select(df.winning_team.alias('winner')).na.drop()
    df2.write.parquet('s3a_bucket/parquet_file', mode= 'overwrite')
    df2.write.format('jdbc').options(
    url = 'jdbc:mysql://database-url',
    driver = 'com.mysql.jdbc.Driver',
    dbtable = 'table',
    user = '',
    password = '').mode("append").save()
    spark.stop()