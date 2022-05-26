from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime



default_args = {"owner": "AdminUser", "start_date": datetime(2022, 4, 2)}

@dag(
    default_args=default_args,
    schedule_interval=None,
    description="Consolidação dos indicadores em Spark no EMR",
    catchup=False,
    tags=["Spark", "EMR"],
)
def processer_consolidate_metrics():
    @task
    def inicio():
        return True

    @task.virtualenv(
        use_dill=True,
        system_site_packages=False,
        requirements=['pandas', 'pyarrow', 'fsspec', 's3fs'],
    )
    def consolidate_metrics(
        aws_access_key_id:str, 
        aws_secret_access_key: str
    ):
        import pandas as pd

        storage_options={
            "key": aws_access_key_id,
            "secret": aws_secret_access_key,
        }

        datatemp_v1 = pd.read_parquet(
            's3://dev-datalake-refined-643626749185/med_salario/',
            storage_options=storage_options,
        )

        datatemp_v2 = pd.read_parquet(
            's3://dev-datalake-refined-643626749185/med_horas_trab/',
            storage_options=storage_options,
        )

        datatemp_v3 = pd.read_parquet(
            's3://dev-datalake-refined-643626749185/med_tempo_trab/',
            storage_options=storage_options,
        )


        resultados = pd.concat(
            [datatemp_v1, datatemp_v2, datatemp_v3],
            axis=1,
            ignore_index=True,
        ).rename(
            columns={
                "0": "media_dalario",
                "1": "media_horas_trabalhadas",
                "2": "media_tempo_trabalhado",
            }
        )
        resultados.columns = resultados.columns.astype(str)
        
        resultados.to_parquet(
            "s3://dev-datalake-refined-643626749185/consolidate_metrics/",
            storage_options=storage_options,
        )

    fim = DummyOperator(task_id="fim")
    
    # Orquestração
    start = inicio()
    indicadores = consolidate_metrics(
        aws_access_key_id = Variable.get("aws_access_key_id"),
        aws_secret_access_key = Variable.get("aws_secret_access_key"),
    )
    start >>indicadores >> fim
    # ---------------


execucao = processer_consolidate_metrics()
