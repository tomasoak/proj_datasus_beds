"""
Explorting the Public Health System Beds' data

`Cadastro Nacional de Estabelecimentos de Saúde (CNES) - Leitos`

Author: Tomás Carvalho
Github: tomasoak
Email: tomas.jpeg@gmail.com
Useful links:
    https://opendatasus.saude.gov.br/dataset/hospitais-e-leitos
    https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/Dicionário_Leito_hospitalar.pdf
"""

from datetime import datetime
import pandas as pd

from airflow.decorators import dag, task


args = {"owner": "Tomas"}

@dag(
    dag_id="extract_transform",
    default_args=args,
    start_date=datetime(2023, 6, 1),
    schedule_interval="@monthly",
)

def extract_transform():
    COLUMNS = ["UF", "REGIAO", "MUNICIPIO", "LEITOS EXISTENTE", "TP_GESTAO",
            "LEITOS SUS", "UTI TOTAL - EXIST", "UTI TOTAL - SUS", "COMP"]

    TP_GESTAO = {
        "M": "Municipal",
        "E": "Estadual",
        "D": "Dupla",
        "S": "Sem gestão"
    }

    @task()
    def extract_mun():
        mun_url = "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv"
        df_mun = pd.read_csv(mun_url, usecols=["codigo_uf", "codigo_ibge", 
                                            "nome", "latitude", "longitude"])

        return df_mun

    @task()
    def extract_datasus_leito():
        df_beds = (pd.concat([pd.read_csv(f"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/Leitos_{year}.csv",
                                       usecols=COLUMNS) for year in range(2007, 2008)], sort=False))
        return df_beds

    @task()
    def load(mun, beds):
        mun.to_parquet("gcs/data/br_municipalities.parquet")
        beds.to_parquet("gcs/data/datasus_leitos.parquet")
        

    mun = extract_mun() 
    beds = extract_datasus_leito()
    load(mun, beds)


extract_transform()
