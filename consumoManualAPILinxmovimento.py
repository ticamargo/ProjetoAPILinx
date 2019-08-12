#!/usr/bin/env python
# coding: utf-8
## @Tiago de Camargo::
### github - https://github.com/ticamargo
### linkedin - https://www.linkedin.com/in/tiagodecamargo/
#### <--- API - LINXMOVIMENTO ---> CONSUMO DE DADOS - ORACLE ####


## IMPORTACAO DE PACOTES
import xml.etree.ElementTree as et
from unicodedata import normalize
from datetime import timedelta, date
import pandas as pd
import cx_Oracle
import requests


## VARIAVEIS
df_cnpj = ['CNPJ','nome_empresa','id_empresas_rede','rede','portal','nome_portal','empresa']
df_cols = ['portal','cnpj_emp','transacao','usuario','documento','chave_nf','ecf','numero_serie_ecf','modelo_nf','data_documento','data_lancamento','codigo_cliente','serie','desc_cfop','id_cfop','cod_vendedor','quantidade','preco_custo','valor_liquido','desconto','cst_icms','cst_pis','cst_cofins','cst_ipi','valor_icms','aliquota_icms','base_icms','valor_pis','aliquota_pis','base_pis','valor_cofins','aliquota_cofins','base_cofins','valor_icms_st','aliquota_icms_st','base_icms_st','valor_ipi','aliquota_ipi','base_ipi','valor_total','forma_dinheiro','total_dinheiro','forma_cheque','total_cheque','forma_cartao','total_cartao','forma_crediario','total_crediario','forma_convenio','total_convenio','frete','operacao','tipo_transacao','cod_produto','cod_barra','cancelado','excluido','soma_relatorio','identificador','deposito','obs','preco_unitario','hora_lancamento','natureza_operacao','tabela_preco','nome_tabela_preco','cod_sefaz_situacao','desc_sefaz_situacao','protocolo_aut_nfe','dt_update','forma_cheque_prazo','total_cheque_prazo','cod_natureza_operacao','preco_tabela_epoca','desconto_total_item','conferido']
url = "https://webapi.microvix.com.br/1.0/api/integracao"
listacnpj_temp = []
lista_cnpj = []
lista_mov = []
dados_cnpj = []
filtro_tag = "D"
ncols_cnpj = 7
ncols_mov = 76
dados_mov = []
cnpj = ''


## SELECIONAR MANUALMENTE AS DATAS DE INICIO E FIM PARA CONSUMO DA API 
## DATA INICIAL - YYYY-MM-DD
hoje = '2019-08-01'
## DATA FINAL
ontem = '2019-08-31'


## CONSULTANDO LISTA DE CNPJ VIA API LINXGRUPOLOJAS
payload_lojas = "<?xml version='1.0' encoding='ISO-8859-1'?> \
<LinxMicrovix> \
<Authentication user='linx_export' password='linx_export'/> \
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
response.status_code
root_cnpj = et.fromstring(response.content)


## TRANSFORMAR EM OBJETO ELEMENT TREE
for child in root_cnpj.iter(filtro_tag):
    listacnpj_temp.append(child.text)


## QUEBRAR EM SUBLISTAS COM TAMANHO DAS COLUNAS NCOLS_CNPJ
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


## CHAMADA PARA CONSUMIR MOVIMENTAÇÕES DAS LOJAS - API LINXMOVIMENTO 
for i in range(len(lista_cnpj)):
    cnpj = lista_cnpj[i].rstrip()
    print(str(i+1) + ', '+ str(lista_cnpj[i]) +'')
    payload = "<?xml version='1.0' encoding='ISO-8859-1'?> \
    <LinxMicrovix> \
    <Authentication user='linx_export' password='linx_export'/> \
    <ResponseFormat>xml</ResponseFormat> \
    <Command> <Name>LinxMovimento</Name> \
    <Parameters> \
    <Parameter id='chave'>BF12D40B-867E-40DD-8F5D-2CD4705F1E73</Parameter> \
    <Parameter id='cnpjEmp'>"+cnpj+"</Parameter> \
    <Parameter id='data_inicial'>"+str(hoje)+"</Parameter> \
    <Parameter id='data_fim'>"+str(ontem)+"</Parameter> \
    </Parameters> \
    </Command> \
    </LinxMicrovix>"
    response = requests.request("POST", url, data=payload)
    root = et.fromstring(response.content)
    for child in root.iter(filtro_tag):
        lista_mov.append(child.text)


## QUEBRAR EM SUBLISTAS COM TAMANHO DAS COLUNAS NCOLS_MOV
for i in range(ncols_mov, len(lista_mov), ncols_mov):
    dados_mov.append(lista_mov[i: i + ncols_mov])


## CRIAR DATAFRAME PARA ORGANIZAR E LIMPAR OS DADOS
dataframe = pd.DataFrame(dados_mov, columns=df_cols)


## ELIMINA LINHAS CUJO STATUS DE PORTAL SEJA PORTAL ELIMINANDO ASSIM OS LINHAS QUE SÃO CABEÇALHOS
## API ENVIA CABEÇALHOS A CADA CONSULTA DE CNPJ
dataframe.drop(dataframe[dataframe.portal=='portal'].index, inplace=True)


## EXPORTAR PARA ARQUIVOCSV, CONFERENCIA E RETENÇÃO DOS DADOS
print(str(ontem)+'_linxmovimento.csv')
dataframe.to_csv(str(ontem)+'_linxmovimento.csv', index=False)

