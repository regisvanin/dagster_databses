from dagster import repository

from oracle_postgre.jobs.say_hello import say_hello_job
from oracle_postgre.jobs.job_clientes import job_cliente
from oracle_postgre.jobs.job_fornecedores import job_fornecedores

from oracle_postgre.schedules.my_hourly_schedule import my_hourly_schedule, cliente_schedule, fornecedor_schedule
from oracle_postgre.sensors.my_sensor import my_sensor


@repository
def oracle_postgre():
    """
    The repository definition for this oracle_postgre Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job, job_cliente, job_fornecedores]
    schedules = [my_hourly_schedule, cliente_schedule, fornecedor_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
