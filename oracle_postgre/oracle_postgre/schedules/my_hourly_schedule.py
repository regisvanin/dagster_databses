from dagster import schedule

from oracle_postgre.jobs.say_hello import say_hello_job
from oracle_postgre.jobs.job_clientes import job_cliente
from oracle_postgre.jobs.job_fornecedores import job_fornecedores


@schedule(cron_schedule="0 * * * *", job=say_hello_job, execution_timezone="US/Central")
def my_hourly_schedule(_context):
    """
    A schedule definition. This example schedule runs once each hour.

    For more hints on running jobs with schedules in Dagster, see our documentation overview on
    schedules:
    https://docs.dagster.io/overview/schedules-sensors/schedules
    """
    run_config = {}
    return run_config

@schedule(cron_schedule="0 * * * *", job=job_cliente, execution_timezone="US/Central")
def cliente_schedule(_context):
    run_config = {}
    return run_config

@schedule(cron_schedule="0 * * * *", job=job_fornecedores, execution_timezone="US/Central")
def fornecedor_schedule(_context):
    run_config = {}
    return run_config