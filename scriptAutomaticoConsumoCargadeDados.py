#!/usr/bin/env python
# coding: utf-8
## @Tiago de Camargo::
### github - https://github.com/ticamargo
### linkedin - https://www.linkedin.com/in/tiagodecamargo/
#### <--- API - LINXMOVIMENTO ---> CARGA DE DADOS - ORACLE ####


## IMPORTACAO DE PACOTES
import xml.etree.ElementTree as et
from unicodedata import normalize
from datetime import timedelta, date
import pandas as pd
import cx_Oracle
import requests
import warnings


## IMPORTAR DATA DO SISTEMA E MOSTRAR
hoje = date.today()
print('Data de Hoje: '+str(hoje))
intervalo = timedelta(1)
ontem = hoje - intervalo
print('Data de Ontem: '+str(ontem))


## VARIAVEIS
df_cnpj = ['CNPJ','nome_empresa','id_empresas_rede','rede','portal','nome_portal','empresa']
df_cols = ['portal','cnpj_emp','transacao','usuario','documento','chave_nf','ecf','numero_serie_ecf','modelo_nf','data_documento','data_lancamento','codigo_cliente','serie','desc_cfop','id_cfop','cod_vendedor','quantidade','preco_custo','valor_liquido','desconto','cst_icms','cst_pis','cst_cofins','cst_ipi','valor_icms','aliquota_icms','base_icms','valor_pis','aliquota_pis','base_pis','valor_cofins','aliquota_cofins','base_cofins','valor_icms_st','aliquota_icms_st','base_icms_st','valor_ipi','aliquota_ipi','base_ipi','valor_total','forma_dinheiro','total_dinheiro','forma_cheque','total_cheque','forma_cartao','total_cartao','forma_crediario','total_crediario','forma_convenio','total_convenio','frete','operacao','tipo_transacao','cod_produto','cod_barra','cancelado','excluido','soma_relatorio','identificador','deposito','obs','preco_unitario','hora_lancamento','natureza_operacao','tabela_preco','nome_tabela_preco','cod_sefaz_situacao','desc_sefaz_situacao','protocolo_aut_nfe','dt_update','forma_cheque_prazo','total_cheque_prazo','cod_natureza_operacao','preco_tabela_epoca','desconto_total_item','conferido']
url = ""
listacnpj_temp = []
lista_cnpj = []
lista_mov = []
dados_cnpj = []
filtro_tag = "D"
ncols_cnpj = 7
ncols_mov = 76
dados_mov = []
cnpj = ''


## CONSULTANDO LISTA DE CNPJ VIA ARQUIVO TEXTO - POIS API NAO ESTA FUNCIONANDO
payload_lojas = "<?xml version='1.0' encoding='ISO-8859-1'?> \
<LinxMicrovix> \
<Authentication user='' password=''/> \
<ResponseFormat>xml</ResponseFormat> \
<Command> \
<Name>LinxGrupoLojas</Name> \
<Parameters> \
<Parameter id='chave'></Parameter> \
<Parameter id='grupo'></Parameter> \
</Parameters> \
</Command> \
</LinxMicrovix>"
response = requests.request("POST", url, data=payload_lojas)
root_cnpj = et.fromstring(response.content)


## TRANSFORMAR EM OBJETO ELEMENT TREE
for child in root_cnpj.iter(filtro_tag):
    listacnpj_temp.append(child.text)


## QUEBRAR EM SUBLISTAS COM TAMANHO DAS COLUNAS N
for i in range(ncols_cnpj, len(listacnpj_temp), ncols_cnpj):
    dados_cnpj.append(listacnpj_temp[i: i + ncols_cnpj])


## CRIAR DATAFRAME PARA ORGANIZAR E LIMPAR OS DADOS
dataframe_cnpj = pd.DataFrame(dados_cnpj, columns=df_cnpj)


## ELIMINAR CNPJS DUPLICADOS VINDOS DA API LINXGRUPOLOJAS
dataframe_cnpj.drop_duplicates(['CNPJ'], keep="last", inplace=True)
dataframe_cnpj = dataframe_cnpj.reset_index(drop=True)


## CRIAR LISTA COM OS CNPJS
for i in dataframe_cnpj.index:
    if dataframe_cnpj.loc[i, 'CNPJ'] != None:
        lista_cnpj.append(dataframe_cnpj.loc[i,'CNPJ'])


## CHAMADA PARA CONSUMIR API
for i in range(len(lista_cnpj)):
    cnpj = lista_cnpj[i].rstrip()
    print(str(i+1) + ', '+ str(lista_cnpj[i]) +'')
    payload = "<?xml version='1.0' encoding='ISO-8859-1'?> \
    <LinxMicrovix> \
    <Authentication user='' password=''/> \
    <ResponseFormat>xml</ResponseFormat> \
    <Command> <Name>LinxMovimento</Name> \
    <Parameters> \
    <Parameter id='chave'></Parameter> \
    <Parameter id='cnpjEmp'>"+cnpj+"</Parameter> \
    <Parameter id='data_inicial'>"+str(ontem)+"</Parameter> \
    <Parameter id='data_fim'>"+str(ontem)+"</Parameter> \
    </Parameters> \
    </Command> \
    </LinxMicrovix>"
    response = requests.request("POST", url, data=payload)
    root = et.fromstring(response.content)
    for child in root.iter(filtro_tag):
        lista_mov.append(child.text)


## QUEBRAR EM SUBLISTAS COM TAMANHO DAS COLUNAS N
for i in range(ncols_mov, len(lista_mov), ncols_mov):
    dados_mov.append(lista_mov[i: i + ncols_mov])


## CRIAR DATAFRAME PARA ORGANIZAR E LIMPAR OS DADOS
dataframe = pd.DataFrame(dados_mov, columns=df_cols)


## ELIMINA LINHAS CUJO STATUS DE PORTAL SEJA PORTAL ELIMINANDO ASSIM OS CABEÇALHOS
## QUE API ENVIA A CADA CONSULTA DE CNPJ
dataframe.drop(dataframe[dataframe.portal=='portal'].index, inplace=True)


## EXPORTAR PARA ARQUIVOCSV, CONFERENCIA E RETENÇÃO DOS DADOS
print(str(ontem)+'_linxmovimento.csv')
dataframe.to_csv(str(ontem)+'_linxmovimento.csv', index=False)


##  CRIAR UM NOVO DATAFRAME DO DATAFRAME COM TODAS AS COLUNAS, SELECIONANDO APENAS AS COLUNAS QUE SERÃO UTILIZADAS
df = dataframe[['portal','cnpj_emp','transacao','documento','chave_nf','ecf','numero_serie_ecf','modelo_nf','data_documento','data_lancamento','codigo_cliente','serie','desc_cfop','id_cfop','cod_vendedor','quantidade','preco_custo','valor_liquido','desconto','valor_total','total_dinheiro','total_cheque','total_cartao','total_crediario','total_convenio','operacao','tipo_transacao','cod_produto','cod_barra','cancelado','excluido','soma_relatorio','identificador','preco_unitario','hora_lancamento','natureza_operacao','cod_sefaz_situacao','desc_sefaz_situacao','protocolo_aut_nfe','total_cheque_prazo','cod_natureza_operacao','preco_tabela_epoca','desconto_total_item']]


# REMOVER TRAÇOS NEGATIVOS EM VALOR_TOTAL PARA FAZER TODA DEVOLUÇÃO NEGATIVA EM VALOR_TOTAL
df.loc[0:, 'valor_total'] = df['valor_total'].str.replace('-', '')


## LIMPAR CARACTERES DA COLUNA COD_NATUREZA_OPERACAO
df.loc[0:, 'cod_natureza_operacao'] = df['cod_natureza_operacao'].str.strip()


## CARREGAR UMA LISTA COM O INDEX ONDE NA COLUNA COD_NATUREZA_OPERACAO = 1.201 PARA ALTERAR PARA NEGATIVO O VALOR_TOTAL 
lista_devolucao = []
for row in df.itertuples():
    if row.cod_natureza_operacao == '1.201':
        lista_devolucao.append(row.Index)


## TRANSFORMAR EM NEGATIVO VALOR_TOTAL E QUANTIDADE DA TUPLA LISTA_DEVOLUCAO
df.loc[lista_devolucao, 'valor_total'] = df['valor_total'].map(lambda x: "-{}".format(x))
df.loc[lista_devolucao, 'quantidade'] = df['quantidade'].map(lambda x: "-{}".format(x))


## PREENCHER NAN
#df.fillna('NULL', inplace=True) #TOTAL, TUDO
values = {'chave_nf': ' ', 'numero_serie_ecf':' ', 'modelo_nf': '0','desc_cfop': ' ', 'id_cfop': '0', 'valor_total': '0', 'cod_barra': '0', 'cod_sefaz_situacao': '0', 'desc_sefaz_situacao': ' ', 'protocolo_aut_nfe': '0', 'desconto_total_item': '0'}
df = df.fillna(value=values)


## RETIRANDO ACENTOS PARA TABELA ASCCI NAS COLUNAS
df.loc[0:, 'desc_cfop'] = df['desc_cfop'].apply(lambda x: normalize('NFKD', x).encode('ascii', 'ignore').decode('ascii'))
df.loc[0:, 'natureza_operacao'] = df['natureza_operacao'].apply(lambda x: normalize('NFKD', x).encode('ascii', 'ignore').decode('ascii'))


## REMOVER TRACINHOS
df.loc[0:, 'data_documento'] = df['data_documento'].str.replace('-', '')
df.loc[0:, 'data_lancamento'] = df['data_lancamento'].str.replace('-', '')


## ACERTANDO FORMATO DE DATA
df.loc[0:, 'data_documento'] = df['data_documento'].map(lambda d: "{}".format(d[0:8]))
df.loc[0:, 'data_lancamento'] = df['data_lancamento'].map(lambda d: "{}".format(d[0:8]))


## CRIAR LISTA DE COLUNAS COLUNAS QUE DEVEM SER DO TIPO INT
cols_int = ['portal','cnpj_emp','transacao','documento','ecf','modelo_nf','codigo_cliente','id_cfop','cod_vendedor','cod_produto','cod_barra','cod_sefaz_situacao','protocolo_aut_nfe']


## CONVERTER PARA INT LISTA DE COLUNAS QUE DEVEM SER DO TIPO INT
df[cols_int] = df[cols_int].apply(lambda x: pd.to_numeric(x.astype('int64'), errors='coerce'))


## PERCORRER COLUNA QUANTIDADE, PRIMEIRO FLOAT, DEPOIS ROUND() DEPOIS INT
df['quantidade'] = df['quantidade'].astype('float64', errors='ignore')
df['quantidade'] = df['quantidade'].round()
df['quantidade'] = df['quantidade'].astype('int64')


## CRIAR LISTA DE COLUNAS COLUNAS QUE DEVEM SER DO TIPO FLOAT
cols_float = ['preco_custo','valor_liquido','desconto','valor_total','total_dinheiro','total_cheque','total_cartao','total_crediario','total_convenio','preco_unitario','total_cheque_prazo','preco_tabela_epoca','desconto_total_item']


## VERIFICAR POR DADOS NAN NO DATAFRAME
df.isnull().sum() != 0


## VERIFICAR TIPOS DE DADOS DAS COLUNAS
df.apply(lambda x: pd.api.types.infer_dtype(x.values))


## CONVERTER PARA FLOAT COLUNAS DE PREÇOS
df[cols_float] = df[cols_float].apply(lambda x: pd.to_numeric(x.astype(str), errors='coerce'))


## MOSTRA O DATAFRAME
df.head()


## CRIA UMA LISTA
df_list = []


## LISTA RECEBE DADOS DO DATAFRAME
df_list = df.values.tolist()


## MOSTRA 1.O ELEMENTO DA LISTA
df_list[lista_devolucao[0]]


## MOSTRA O TAMANHO DA LISTA
print(len(df_list))


## STRING DE CONEXÃO ORACLE
connect = cx_Oracle.connect('')


## INSERIR NA BASE DE DADOS - METODO MATRIZ
rows = df_list
cursor = connect.cursor()
cursor.bindarraysize = 43
cursor.setinputsizes(int,int,int,int,200,int,50,int,8,8,int,50,300,int,int,int,float,float,float,float,                     float,float,float,float,float,2,1,int,int,1,1,1,50,float,8,50,int,50,int,float,15,float,float)
statement = 'INSERT INTO DW_LINX (portal,cnpj_emp,transacao,documento,chave_nf,ecf,numero_serie_ecf,modelo_nf,data_documento,data_lancamento,codigo_cliente,serie,desc_cfop,id_cfop,cod_vendedor,quantidade,preco_custo,valor_liquido,desconto,valor_total,total_dinheiro,total_cheque,total_cartao,total_crediario,total_convenio,operacao,tipo_transacao,cod_produto,cod_barra,cancelado,excluido,soma_relatorio,identificador,preco_unitario,hora_lancamento,natureza_operacao,cod_sefaz_situaca,desc_sefaz_situacao,protocolo_aut_nfe,total_cheque_prazo,cod_natureza_operacao,preco_tabela_epoca,desconto_total_item) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43)'
for i in range(0, len(rows), 10000):
    try:
        cursor.executemany(statement, rows[i:i+10000])
        connect.commit()
        print('Range :::', i ,'Até:::', i+9999 ,'::: INSERIDO/COMMIT COM SUCESSO :::')
    except cx_Oracle.DatabaseError as e:
        errorObj, = e.args
        print("NO RANGE ", i ,"A LINHA ", cursor.rowcount, "ESTÁ COM ERRO ", errorObj.message)
        connect.rollback()
        cursor.close()
        connect.close()
        break

cursor.close()
connect.close()
print('Tudo OK!!')