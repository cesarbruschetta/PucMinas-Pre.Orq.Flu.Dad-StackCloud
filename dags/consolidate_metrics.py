from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime

import pandas as pd

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
        requirements=['pandas'],
    )
    def consolidate_metrics(success_before: bool):
        datatemp_v1 = pd.read_parquet(
            's3://dev-datalake-refined-643626749185/med_salario/'
        )

        datatemp_v2 = pd.read_parquet(
            's3://dev-datalake-refined-643626749185/med_horas_trab/'
        )

        datatemp_v3 = pd.read_parquet(
            's3://dev-datalake-refined-643626749185/med_tempo_trab/'
        )


        resultados = pd.concat(
            [indicador_01, indicador_02, indicador_03],
            axis=1,
            ignore_index=True,
        ).rename(
            columns={
                0: "media_dalario",
                1: "media_horas_trabalhadas",
                2: "media_tempo_trabalhado",
            }
        )

        resultados.to_parquet("s3://dev-datalake-refined-643626749185/consolidate_metrics/")

    # Orquestração
    start = inicio()
    indicadores = consolidate_metrics(start)
    start >>indicadores >> fim
    # ---------------


execucao = processer_consolidate_metrics()
