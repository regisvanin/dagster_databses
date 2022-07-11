from dagster import RunRequest, sensor

from oracle_postgre.jobs.say_hello import say_hello_job
from oracle_postgre.jobs.job_clientes import job_cliente
from oracle_postgre.jobs.job_fornecedores import job_fornecedores


@sensor(job=say_hello_job)
def my_sensor(_context):
    """
    A sensor definition. This example sensor always requests a run at each sensor tick.

    For more hints on running jobs with sensors in Dagster, see our documentation overview on
    sensors:
    https://docs.dagster.io/overview/schedules-sensors/sensors
    """
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})



@sensor(job=job_cliente)
def my_sensor(_context):
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})

@sensor(job=job_fornecedores)
def my_sensor(_context):
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})
