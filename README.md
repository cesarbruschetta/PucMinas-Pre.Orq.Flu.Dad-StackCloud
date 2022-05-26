# MBA PUC Minas - Atividade 4 - Stack de ED em nuvem

## Enuncioado Trabalho Final

O que deve ser feito:

- Na AWS
    - Criar um Bucket S3
    - Criar um Cluster EMR
    - Criar um cluster Kubernetes
    - Deployar o Airflow no cluster Kubernetes
    - Criar um usuário chamado `airflow-user` com permissão de administrador da conta

- No Airflow 
    - Escolher um dataset (livre escolha)
    - Subir o dataset em um bucket S3
    - Pensar e implementar construção de indicadores e análises sobre esse dataset (produzir 2 ou mais indicadores no mesmo grão)
    - Escrever no S3 arquivos parquet com as tabelas de indicadores produzidos
    - Escrever outro job spark que lê todos os indicadores construídos, junta tudo em uma única tabela
    - Escrever a tabela final de indicadores no S3

- No AWS EMR Notebook
    - Subir um notebook dentro do cluster EMR
    - Ler a tabela final de indicadores e dar um `.show()`

- Todo o processo orquestrado pelo AIRFLOW no K8s


## Instalaçao do cluster K8S

```shell
$ eksctl create cluster \
    --name kubepucminas \
    --managed --full-ecr-access --asg-access --spot \
    --node-private-networking --alb-ingress-access \
    --instance-types=m5.xlarge \
    --region=us-east-1 \
    --nodes-min=2 \
    --nodes-max=3 \
    --nodegroup-name=ng-kubepucminas \
    --version=1.21
```

## Instalaçao do Apache Airflow

```shell
$ helm repo add apache-airflow https://airflow.apache.org && helm repo update

$ helm upgrade \
    --install airflow \
    apache-airflow/airflow \
    --values kubernetes/custom_values.yaml \
    --namespace airflow \
    --create-namespace \
    --debug

```


## Copiar arquivos spark para o S3

```shell
$ aws s3 cp ./pyspark s3://dev-datalake-artifact-643626749185/pyspark/
```


## License

MBA em Engenharia de Dados, PUC Minas, Maio 2022.

