from dagster import op, graph, job
from sqlalchemy import create_engine
import psycopg2 as psy
import cx_Oracle as ora
import pandas as pd

@op(config_schema={'host':str, 'user':str, 'sid':str, 'password':str})
def delete_forencedor(context):
    """ Rotina destinada a limpeza da t_fornecedores do Postgres."""
    host = context.op_config['host']
    user = context.op_config['user']
    sid = context.op_config['sid']
    password = context.op_config['password']
    eng = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/{sid}")
    with eng.connect() as conn:
        conn.execute("drop table if exists t_fornecedores;")
        conn.close()

@op(config_schema={'host':str, 'user':str, 'sid':str, 'password':str})
def get_fornecedor(context,data):
    """Rotina destina a capturar os dados de Fornecedores ."""
    host = context.op_config['host']
    user = context.op_config['user']
    sid = context.op_config['sid']
    password = context.op_config['password']
    eng = create_engine(f"oracle+cx_Oracle://{user}:{password}@{host}:1521/{sid}")
    with eng.connect() as conn:
        df = pd.read_sql('select id, descritivo, fantasia, cnpj_cpf from fornecedores', conn)
        conn.close
    return df

@op
def transf_fornecedor(data):
    """Rotina destinada a tratar as informações de Fornecedores."""
    transformed_fornecedor = data.rename(colmuns={
        "id":"fornecedor_id",
        "descritivo":"fornecedor_descritivo"
    })
    return transformed_fornecedor

@op(config_schema={'host':str, 'user':str, 'sid':str, 'password':str})
def load_fornecedor(context, data):
    """Rotina destina a subir o cadastro de fornecedores para o Postgres."""
    host = context.op_config['host']
    user = context.op_config['user']
    sid = context.op_config['sid']
    password = context.op_config['password']
    eng = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/{sid}")
    with eng.connect() as conn:
        data.to_sql('t_fornecedores', con=conn, if_exists='replace', index=False)
        conn.close()
        
@graph
def run_fornecedores():
    load_fornecedor(transf_fornecedor(get_fornecedor(delete_forencedor())))


# CONFIGURAÇÕES E PARAMETROS
run_config = {
    "ops":{
        "delete_forencedor":{
            "config":{
                "host":"localhost",
                "password":"123456",
                "sid":"postgres",
                "user":"postgres"
            }
        },
        "get_fornecedor":{
            "config":{
                "host":"192.168.56.103",
                "password":"automa",
                "sid":"arius",
                "user":"proreg"
            }
        },
        "load_fornecedor":{
            "config":{
                "host":"192.168.56.103",
                "password":"automa",
                "sid":"arius",
                "user":"proreg"
            }
        }
    }
}


# CRIA O JOB
# run_fornecedores_job = run_fornecedores.to_job()

# if __name__ == '__main__':
#     result = run_fornecedores_job.execute_in_process(run_config=run_config)