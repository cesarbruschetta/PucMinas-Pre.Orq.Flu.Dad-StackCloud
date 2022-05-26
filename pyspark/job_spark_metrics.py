from pyspark.sql import functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Processamento dos indicadores").getOrCreate()

raisparquet = (
    spark
    .read
    .parquet('s3://dev-datalake-raw-643626749185/rais/')
)

# Calculando os indicadores

datatemp_v1 = (
    raisparquet
    .filter(raisparquet.vl_remun_media_nom > 0.0)
    .groupBy(
        'sigla_uf', 
        'nome_sexo', 
    )
    .agg(
        f.mean("vl_remun_media_nom").alias("med_salario")
    )
    .withColumn(
        'med_salario_format',
        f.format_number('med_salario', 2)
    )
    .orderBy(
        'sigla_uf', 
        'nome_sexo', 
        f.col("med_salario").desc()
    )
    .select(
        'sigla_uf', 
        'nome_sexo', 
        'med_salario_format'
    )
)

datatemp_v2 = (
    raisparquet
    .filter(raisparquet.qtd_hora_contr > 0.0)
    .groupBy(
        'sigla_uf', 
        'nome_sexo', 
    )
    .agg(
        f.mean("qtd_hora_contr").alias("med_horas_trab")
    )
    .withColumn(
        'med_horas_trab_format',
        f.format_number('med_horas_trab', 2)
    )
    .orderBy(
        'sigla_uf', 
        'nome_sexo', 
        f.col("med_horas_trab_format").desc()
    )
    .select(
        'sigla_uf', 
        'nome_sexo', 
        'med_horas_trab_format'
    )
)

datatemp_v3 = (
    raisparquet
    .withColumn("tempo_emprego", f.regexp_replace("tempo_emprego", ',', '.').cast('double'))
    .filter(f.col('tempo_emprego') > 0.0)
    .groupBy(
        'sigla_uf', 
        'nome_sexo', 
    )
    .agg(
        f.mean("tempo_emprego").alias("med_tempo_trab")
    )
    .withColumn(
        'med_tempo_trab_format',
        f.format_number(
            f.col('med_tempo_trab') / 12,
            2
        )
    )
    .orderBy(
        'sigla_uf', 
        'nome_sexo', 
        f.col("med_tempo_trab_format").desc()
    )
    .select(
        'sigla_uf', 
        'nome_sexo', 
        'med_tempo_trab_format'
    )
)

# Salvando os datasets de indicadores

(
    datatemp_v1
    .coalesce(50)
    .write
    .mode('overwrite')
    .partitionBy('sigla_uf')
    .format('parquet')
    .save('s3://dev-datalake-refined-643626749185/med_salario/')
)

(
    datatemp_v2
    .coalesce(50)
    .write
    .mode('overwrite')
    .partitionBy('sigla_uf')
    .format('parquet')
    .save('s3://dev-datalake-refined-643626749185/med_horas_trab/')
)

(
    datatemp_v3
    .coalesce(50)
    .write
    .mode('overwrite')
    .partitionBy('sigla_uf')
    .format('parquet')
    .save('s3://dev-datalake-refined-643626749185/med_tempo_trab/')
)



