from dagster import op, job, get_dagster_logger, graph
import cx_Oracle as ora
import psycopg2 as psy
import pandas as pd
from sqlalchemy import create_engine

@op
def delete_cliente():
    """
        Rotina que apaga a tabela "t_clientes" .
    """
    eng = create_engine("postgresql+psycopg2://postgres:123456@localhost:5432/postgres")
    if eng:
        conn = eng.connect()
        conn.execute("drop table if exists postgres.t_clientes;")
        conn.close()

@op
def get_cliente(delete):
    """   
    Rotina para capturar os clientes e selecionar somente as 3 colunas (ID, DESCRITIVO, CPF)
    """
    eng = create_engine("oracle+cx_oracle://proreg:automa@192.168.56.103:1521/Arius")
    if eng:
        clientes = pd.read_sql("SELECT id, descritivo, cnpj_cpf, categoria FROM clientes", eng)
    return clientes

@op(config_schema={'host':str, 'user':str, 'sid':str, 'password':str})
def get_categoria(context):
    """   
    Rotina para capturar as categorias dos clientes.
    """
    host = context.op_config['host']
    user = context.op_config['user']
    password = context.op_config['password']
    sid = context.op_config['sid']
    eng = create_engine(f"oracle+cx_oracle://{user}:{password}@{host}:1521/{sid}")
    if eng:
        categoria = pd.read_sql("SELECT id, descritivo FROM clientes_categorias", eng)
    return categoria

@op
def transform_cliente(cliente, categoria):
    """
        Rotina que trata a nomenclatura das coludas .:
        ID -> CODIGO_CLIENTE
        DESCRITIVO -> NOME_CLIENTE
    """
    transf_data = cliente.rename(columns={'id':'codigo_cliente'
                                          ,'descritivo':'nome_cliente'
                                          ,'cnpj_cpf':'cnpj_cpf'
                                          ,'categoria':'id_categoria_cliente'
                                          })
    transf_categoria = categoria.rename(columns={'id':'id_categoria_cliente'
                                                  ,'descritivo':'desc_categoria'
                                                  })
    transf_full = pd.merge(transf_data, transf_categoria,left_on="id_categoria_cliente", right_on="id_categoria_cliente")
    return transf_full

@op
def load_cliente(cliente):
    """
        Rotina que grava as informações tratadas no Postgrasql na tabela "t_clientes" .
    """
    eng = create_engine("postgresql+psycopg2://postgres:123456@localhost:5432/postgres")
    if eng:
        conn = eng.connect()
        cliente.to_sql('t_clientes', con=conn, if_exists='replace', index=False)
        conn.close()
        
# CONFIGURAÇÕES E PARAMETROS
run_config = {
    "ops":{
        "get_categoria":{
            "config":{
                "host":"192.168.56.103",
                "password":"automa",
                "sid":"Arius",
                "user":"proreg"
            }
        }
    }
}

# CRIA O  GRAPH COM AS DEPENDENCIAS DOS OPS.
@graph
def run_cliente():
    load_cliente(transform_cliente(get_cliente(delete_cliente()), get_categoria()))

# CRIANDO UM JOB
# run_cliente_job = run_cliente.to_job()


# if __name__ == '__main__':
#     result = run_cliente_job.execute_in_process(run_config=run_config)