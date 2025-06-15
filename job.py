from pyspark.sql import SparkSession

print('1'*70)
spark = SparkSession.builder.appName("job-simple").getOrCreate()
print('2'*70)

df = spark.range(10).withColumnRenamed("id", "numero")
print('3'*70)

df = df.withColumn("cuadrado", df.numero * df.numero)
print('4'*70)

df.show()
print('5'*70)

output_path = "s3://proyecto-1-ml/output/job_simple"
df.write.mode("overwrite").parquet(output_path)

print('6'*70)
spark.stop()
print('7'*70)
