import oracledb
import credenciais as cr
import pandas as pd
from re import findall, IGNORECASE
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

#dados para consulta do banco
dns_base = oracledb.makedsn(host=cr.host, port = cr.port,service_name=cr.banco)

connection = oracledb.connect(dsn = dns_base, user = cr.login, password = cr.senha)


#condicional para n exportar todos os dados

#def pagamentos():
#
#  data_fim = pd.Timestamp(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
#
#  if data_fim.day > 13:
#    data_inicio = pd.Timestamp(datetime(ano, data_fim.month, 1))
#  else:
#    if data_fim.month == 1:
#      data_inicio = pd.Timestamp(datetime(ano, data_fim.month, 1))
#    else:
#      data_inicio = pd.Timestamp(datetime(ano, data_fim.month-1, 1))
#
  #acessando e obtendo a tabela

ano = 2024
data_inicio = pd.Timestamp(datetime(ano, 1, 1))
data_fim = pd.Timestamp(datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
query = f'''
SELECT 
NUORDEMBANCARIA, NUDOCUMENTO, CDSITUACAOPREPARACAOPAGAMENTO, 
NUNOTAEMPENHO, DTLANCAMENTO, NUPROCESSO, IDCREDOR, DEOBSERVACAO, 
CDNATUREZADESPESA, CDMODALIDADE, VLTOTAL 
FROM sigef{ano}.VSEPLANOBSAU 
WHERE CDGESTAO IN 21901
AND DTLANCAMENTO BETWEEN TO_TIMESTAMP('{data_inicio}', 'YYYY-MM-DD HH24:MI:SS.FF3') 
AND TO_TIMESTAMP('{data_fim}', 'YYYY-MM-DD HH24:MI:SS.FF3')
'''

q_cred = f'''
SELECT IDCREDOR, NMCREDOR
FROM sigef{ano}.VSEPLANCREDORSAU
'''

#  with open('historico.txt', 'w') as arquivo:
#    arquivo.write(data_fim)
#  return pd.read_sql_query(query, con = connection)

base_pg = pd.read_sql_query(query, con = connection)
credor = pd.read_sql_query(q_cred, con = connection)

dados = pd.merge(base_pg, credor, on = 'IDCREDOR', how = 'left')

#tratando
dados['DEOBSERVACAO'] = dados['DEOBSERVACAO'].replace({'//':'/', ',':''}, regex = True)
dados['CONFIR PGTO'] = 0
dados['STS PGTO'] = 'PAGO'
dados['RETENCAO'] = ""
dados['DETALHE_RETENCAO'] = ""


for i in range(len(dados)):
  #separando o texto das observações
  texto = dados.at[i, "DEOBSERVACAO"]
  #tratando o as observações com imposto
  imposto = findall(r'\bir\b|\birr\b|\birrf\b|\birrffolha\b|\biptu\b|\binss\b|\biss\b', texto, flags = IGNORECASE)
       
  if len(imposto) >= 1:

    dados.at[i,'RETENCAO'] = "SIM"
    dados.at[i, 'DETALHE_RETENCAO'] = '/'.join(imposto)
  
  else:
     dados.at[i,'RETENCAO'] = "NAO"
     pass

  #numero do processo considerando vários
  
  ultima_letra = findall(r'[a-zA-Z\;\:]', texto)[-1] #encontra a ultima letra ou ';' ou '.' da string
  posição = texto.rfind(ultima_letra)
  processos = texto[posição:]
  l_processos = findall(r'(\d{3,7}/\d{2,4}|\d{5,7}|\d{4}\.\d{6}\.\d{5}|\d{15})', processos)

  try:
    posição_barra = l_processos[-1].rfind('/')
    ano_proc = l_processos[-1][posição_barra:]
  except:
    pass

  "retorna o processo se a barra existir, caso contrário unirá com a string ano, e fará isso para cada processo"
  l_processos_corrigidos = [iten if '/' in iten else iten+ano_proc for iten in l_processos]


  if len(l_processos) > 1:

    filtro = dados['DEOBSERVACAO'].str.contains(' '.join(l_processos), case=False, regex = True)#filtrando as obs com a lista de processos

    for k in range(len(l_processos)):
      try:
        linha = dados[filtro]['CONFIR PGTO'].index[k]#obtendo as linhas que contem os processos
      
        dados.at[linha, "CONFIR PGTO"] = l_processos_corrigidos[k]#escrevendo cada processo nas respectivas linhas
      except:
        dados.at[i, "CONFIR PGTO"] = dados.at[i, "NUPROCESSO"]
        pass
  else:
    try:
      dados.at[i,'CONFIR PGTO'] = l_processos[0].lstrip("0")
    except:
      try:
        proc = findall(r'(\d{1,11})', texto)[-1].lstrip("0")
        dados['CONFIR PGTO'].iat[i] = proc[-4:]+"/"+proc[:-4].lstrip("0")
      except:
        dados['CONFIR PGTO'].iat[i] = 'erro'

dados['CONFIR PGTO'] = dados['CONFIR PGTO'].replace({'/22':'/2022', '/23':'/2023'}, regex = True)

#exportando para excel e arrumando as bases

mes_ant = pd.Timestamp(datetime(data_fim.year, data_fim.month-1, 1))#criando uma variável para o mes anterior
arquivo_ant = f'{cr.pasta}\\Historico_{mes_ant.month_name()}_{data_fim.month_name()}.xlsx' #nome do arquivo com dois meses que existirá até o dia 13
arquivo_unico = f'{cr.pasta}\\Historico_{mes_ant.month_name()}.xlsx' # fruto de um só mes de pagamentos

if data_fim.day > 13:
  
  try:#se o arquivo do mes anterior (dois meses juntos) existir o código será executado (uma vez).
    
    arquivo_nov = f'{cr.pasta}\\Historico_{mes_ant.month_name()}.xlsx'
    arrumar = pd.read_excel(arquivo_ant)
    arrumar[arrumar['DTLANCAMENTO'] < data_inicio].to_excel(arquivo_nov, index = False)#salvando só um mes como histórico
    os.remove(arquivo_ant)#excluindo o arquivo anterior
  
  except:
    pass
  
  dados.to_excel(f'{cr.pasta}\\Historico_{data_fim.month_name()}.xlsx', index=False)

else:
  
  try:#excluindo (se existir) arquivo do mes cheio. logo será excluido uma vez se antes do dia 13 de cada mes
    os.remove(arquivo_unico)
  
  except:
    pass

  dados.to_excel(arquivo_ant, index=False)#uma base de dois meses, uma vez que o mes anterior até o dia 13 passa por mudanças
  

'''obs: não concatenei os dfs do mes anterior pq antes do dia 13 o mes anterior é 'vivo' 
e de possível alteração logo ele n pode ser estático e existir uma única vez.
'''

#unindo em uma só base

pasta  = os.listdir(cr.pasta)
pagamentos = pd.DataFrame()


for arquivos in pasta :
  if 'PAGAMENTOS' in arquivos:
    pass
  else:
    b = pd.read_excel(cr.pasta+'\\'+arquivos)
    pagamentos = pd.concat([pagamentos, b], ignore_index=True)


pagamentos = pagamentos.reindex(columns=['CONFIR PGTO',	'NUORDEMBANCARIA',	'NUDOCUMENTO',	'STS PGTO',	
                            'CDSITUACAOPREPARACAOPAGAMENTO',	'NUNOTAEMPENHO',	'DTLANCAMENTO',	'NUPROCESSO',
                              'IDCREDOR','NMCREDOR','DEOBSERVACAO',	'CDNATUREZADESPESA',	'CDMODALIDADE',	
                              'VLTOTAL',	'RETENCAO','DETALHE_RETENCAO'])

pagamentos.to_excel(f"{cr.pasta_final}/PAGAMENTOS.xlsx", index = False)
print("tudo ok")