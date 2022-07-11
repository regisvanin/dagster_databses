from dagster import job
from oracle_postgre.ops.clientes import run_cliente

@job
def job_cliente():
    run_cliente()