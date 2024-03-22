import oracledb
import credenciais as cr
import pandas as pd
from datetime import datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')

hoje = pd.Timestamp(datetime.today())
hoje_hora = f'{hoje.date().strftime("%d-%m-%Y")}_{hoje.time().strftime("%Hh%Mm%Ss")}'
print(hoje_hora)
#dados para consulta do banco
dns_base = oracledb.makedsn(host=cr.host, port = cr.port,service_name=cr.banco)
connection = oracledb.connect(dsn = dns_base, user = cr.login, password = cr.senha)

q_emp = '''SELECT NUNEORIGINAL,NUPROCESSO,
IDCREDOR,CDNATUREZADESPESA,CDMODALIDADELICITACAO
FROM sigef2024.VSEPLANNESAU
WHERE CDGESTAO IN 21901'''

q_orc = '''SELECT *
FROM sigef2024.VSEPLANORSAU
WHERE CDGESTAO IN 21901'''

q_pre = '''SELECT NUPROCESSO, NUPREEMPENHO, CDSUBACAO, DTREFERENCIA,
CDIDENTIFICADORUSO, CDFONTE, CDNATUREZADESPESA, SALDO_PRE_EMPENHO
FROM sigef2024.VSEPLANPESAU
WHERE CDUNIDADEORCAMENTARIA IN 21901'''

q_suba = '''SELECT CDSUBACAO, NMSUBACAO 
FROM sigef2024.VSEPLANSUBACAOSAU'''

q_cred = '''SELECT IDCREDOR, NMCREDOR
FROM sigef2024.VSEPLANCREDORSAU
'''


"""## arrumando base"""

emp = pd.read_sql_query(q_emp, con = connection)
print('query ok')
orc = pd.read_sql_query(q_orc, con = connection)
print('query ok')
pre = pd.read_sql_query(q_pre, con = connection)
print('query ok')
suba = pd.read_sql_query(q_suba, con = connection)
print('query ok')
credor = pd.read_sql_query(q_cred, con = connection)

print('query ok all')

"""OS PREEMPENHOS COM SALDOS ZERADOS SÃO OS QUE FORAM ANULADOS"""

pre = pre[pre['SALDO_PRE_EMPENHO'] != '0']#removendo os zerados

emp = emp.drop_duplicates()

orc.rename(columns={'NUNOTAEMPENHO':'NUNEORIGINAL','NUPREEMPENHOORIGINAL':'NUPREEMPENHO'}, inplace = True)
orc = orc.drop(columns=['NUNOTAEMPENHOORIGINAL'])

orc = pd.merge(orc, emp,how='left', on='NUNEORIGINAL')

#transformando o tipo numérico
colunas_valor = ['VLEMPENHADO', 'VLLIQUIDADO','VLLIQUIDAR', 'VLPAGO', 'VPAGAR']
#for colunas in colunas_valor:
#  orc[colunas] = orc[colunas].str.replace('.', '', regex=True).str.replace(',', '.', regex=True).astype(np.float64)
#  #orc[colunas].replace({'\.':'',',':'.'}, regex = True, inplace = True)
#  #orc[colunas] = orc[colunas].astype(float)


"""#Preempenho"""

pre_orc = orc[['NUPREEMPENHO','VLEMPENHADO', 'VLLIQUIDADO','VLLIQUIDAR', 'VLPAGO', 'VPAGAR']]
pre_orc = pre_orc.groupby(by=['NUPREEMPENHO'],as_index = False).sum() #resumindo a execução para cada preempenho

pre2 = pd.merge(pre, pre_orc, on='NUPREEMPENHO', how='left')

#pre2['CDSUBACAO'] = pre2['CDSUBACAO']*1000
#pre2['CDSUBACAO'] = pre2['CDSUBACAO'].astype(int)#mudando o tipo de dados na planilha orc

pre2 = pd.merge(pre2, suba, on = 'CDSUBACAO', how = 'left')

pre2 = pre2.reindex(columns=['NUPROCESSO', 'NUPREEMPENHO', 'DTREFERENCIA',
       'CDSUBACAO', 'NMSUBACAO', 'CDIDENTIFICADORUSO', 'CDFONTE','CDNATUREZADESPESA', 'SALDO_PRE_EMPENHO', 
       'VLEMPENHADO', 'VLLIQUIDADO', 'VLLIQUIDAR','VLPAGO', 'VPAGAR'
       ])

#pre2.to_csv(f'preempenho{hoje_hora}.csv', sep='@', index=False, decimal=',')
pre2.to_excel(f'{cr.pasta_orc2024}\\preempenho{hoje_hora}.xlsx', index=False)

"""colocando nome das subações e dos credores na base de orçamento

## orçamento
"""

pre_dl = pre2[['NUPREEMPENHO', 'DTREFERENCIA']]#criando df só com a data lançamento do pre
orc = pd.merge(orc, suba, on = 'CDSUBACAO', how = 'left')
orc = pd.merge(orc, credor, on = 'IDCREDOR', how = 'left')
orc = pd.merge(orc, pre_dl, on = 'NUPREEMPENHO', how='left')#colocando a data dereferencia do pe

col_nome = ['NUPROCESSO','CDUNIDADEGESTORA', 'CDGESTAO', 'NUPREEMPENHO', 'DTREFERENCIA',
            'NUNEORIGINAL','DTLANCAMENTO','IDCREDOR','NMCREDOR', 'CDEVENTO', 'TIPO',
            'CDGRUPOPROGFINANCEIRA', 'CDSUBACAO', 'NMSUBACAO','CDUNIDADEORCAMENTARIA',
            'CDNATUREZADESPESA_y','CDMODALIDADELICITACAO', 'CDFONTE']

col_valor = ['VLEMPENHADO', 'VLLIQUIDADO','VLLIQUIDAR', 'VLPAGO', 'VPAGAR']
orc = orc.reindex(columns=col_nome+col_valor)

#orc_2 = orc.groupby(by=col_nome, as_index = False).sum() #resumindo a execução para cada preempenho

#orc.to_csv(f'orçamento{hoje_hora}.csv', sep='@',index = False, decimal=',')
orc.to_excel(f'{cr.pasta_orc2024}\\orçamento{hoje_hora}.xlsx', index = False)
