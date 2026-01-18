import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from enviar_email import EnviarEmail

# Configurações padrão da DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Definir DAG
dag = DAG(
    'analise_rfm_dag',
    default_args=default_args,
    description='DAG para análise RFM de clientes',
    schedule='0 21 * * *',
    catchup=False
)

def conectar_banco():
    """Conectar ao PostgreSQL""" #Usar banco disponibilizado na aula ou testar com banco remoto feito para testes
    conn = psycopg2.connect(
        host='',
        database='',
        user='',
        password='
    )
    return conn

def extrair_dados(**context):
    """Extrair dados de vendas"""
    conn = conectar_banco()
    
    query = """
    select nf.id, nf.data_venda::date as data, nf.valor , COALESCE(pf.cpf, pj.cnpj) AS cpf_cnpj 
    from vendas.nota_fiscal nf 
    left join geral.pessoa_fisica pf on pf.id = nf.id_cliente 
    left join geral.pessoa_juridica pj on pj.id = nf.id_cliente
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f'DataFrame criado com {len(df)} registros')
    return df.to_json()

def calcular_rfm(**context):
    """Calcular métricas RFM"""
    df_json = context['task_instance'].xcom_pull(task_ids='extrair_dados')
    df = pd.read_json(df_json, orient='records')
    df['data'] = pd.to_datetime(df['data'])
    
    data_referencia = df['data'].max()
    
    rfm = df.groupby('cpf_cnpj').agg({
        'data': lambda x: (data_referencia - x.max()).days,
        'id': 'count',
        'valor': 'sum'
    }).reset_index()
    
    rfm.columns = ['cpf_cnpj', 'recencia', 'frequencia', 'valor_monetario']
    
    # Criar scores RFM com tratamento de duplicatas
    try:
        rfm['r_score'] = pd.qcut(rfm['recencia'], 5, labels=[5,4,3,2,1], duplicates='drop')
    except:
        rfm['r_score'] = pd.cut(rfm['recencia'], 5, labels=[5,4,3,2,1])
    
    try:
        rfm['f_score'] = pd.qcut(rfm['frequencia'].rank(method='first'), 5, labels=[1,2,3,4,5], duplicates='drop')
    except:
        rfm['f_score'] = pd.cut(rfm['frequencia'], 5, labels=[1,2,3,4,5])
    
    try:
        rfm['m_score'] = pd.qcut(rfm['valor_monetario'], 5, labels=[1,2,3,4,5], duplicates='drop')
    except:
        rfm['m_score'] = pd.cut(rfm['valor_monetario'], 5, labels=[1,2,3,4,5])
    
    rfm['rfm_score'] = rfm['r_score'].astype(str) + rfm['f_score'].astype(str) + rfm['m_score'].astype(str)
    
    print(f'RFM calculado para {len(rfm)} clientes')
    return rfm.to_json()

def segmentar_clientes(**context):
    """Segmentar clientes baseado no RFM"""
    rfm_json = context['task_instance'].xcom_pull(task_ids='calcular_rfm')
    rfm = pd.read_json(rfm_json, orient='records')
    
    def segmentar_cliente(row):
        r, f, m = int(row['r_score']), int(row['f_score']), int(row['m_score'])
        
        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        elif r >= 3 and f >= 3 and m >= 3:
            return 'Loyal Customers'
        elif r >= 4 and f <= 2:
            return 'New Customers'
        elif r <= 2 and f >= 3 and m >= 3:
            return 'At Risk'
        elif r <= 2 and f <= 2:
            return 'Lost'
        else:
            return 'Potential Loyalists'
    
    rfm['segmento'] = rfm.apply(segmentar_cliente, axis=1)
    
    segmentos = rfm.groupby('segmento').agg({
        'cpf_cnpj': 'count',
        'recencia': 'mean',
        'frequencia': 'mean',
        'valor_monetario': 'mean'
    }).round(2)
    
    segmentos.columns = ['qtd_clientes', 'recencia_media', 'frequencia_media', 'valor_medio']
    segmentos['percentual'] = (segmentos['qtd_clientes'] / len(rfm) * 100).round(1)
    
    print('Segmentação concluída')
    return segmentos.to_json()

def enviar_relatorio(**context):
    """Enviar relatório por email"""
    segmentos_json = context['task_instance'].xcom_pull(task_ids='segmentar_clientes')
    segmentos = pd.read_json(segmentos_json, orient='records')
    
    email_sender = EnviarEmail(
        smtp_server='smtp.gmail.com',
        porta=587,
        email_remetente='',
        senha=''
    )
    
    email_sender.enviar(
        assunto='Relatório RFM - Análise de Clientes',
        df=segmentos,
        destinatario=''
    )
    
    print('Email enviado com sucesso!')

# Definir tasks
task_extrair = PythonOperator(
    task_id='extrair_dados',
    python_callable=extrair_dados,
    dag=dag
)

task_rfm = PythonOperator(
    task_id='calcular_rfm',
    python_callable=calcular_rfm,
    dag=dag
)

task_segmentar = PythonOperator(
    task_id='segmentar_clientes',
    python_callable=segmentar_clientes,
    dag=dag
)

task_email = PythonOperator(
    task_id='enviar_relatorio',
    python_callable=enviar_relatorio,
    dag=dag
)

# Definir dependências
task_extrair >> task_rfm >> task_segmentar >> task_email
