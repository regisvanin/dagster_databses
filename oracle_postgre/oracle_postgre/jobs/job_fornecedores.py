from dagster import job
from oracle_postgre.ops.fornecedores import run_fornecedores

@job
def job_fornecedores():
    run_fornecedores()