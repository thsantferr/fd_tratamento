# -*- coding: utf-8 -*-
"""
Spyder Editor

Thales S. Ferreira

"""
import pandas as pd
import os
import sys
import json
import numpy as np

'''
importar cabeçalho
esta em um subpasta dentro dos codigos
'''
js_file = open(r'.\cab\cab_rev.json')
cab = json.load(js_file)
js_file.close()


# func criação de df
def criar_df(lst_txt):
    '''
    tratamento de dados coletados
    ---------------------------------------
    inserir variavel com a lista de valores
    retorna dicionario com valores de dataframe tratados
    '''
    # Retirada transformar em dataframe
    df = pd.DataFrame(lst_txt)
    # tratamento de dados, limpeza de linhas
    df = df[~df[1].isna()]
    df = df[df[0] == ""]
    # df.drop(columns=[0],inplace=True)
    df.reset_index(inplace=True, drop=True)
    dic = {}
    dic = {'orig': df}
    # criar dicionario a partir dos tipos de linhas:
    for i in df[1].drop_duplicates():
        if i != None:
            # salvar modulo no dicionario
            dic[str(i).lower()] = df[df[1] == i]
            # retirar colunas em branco.
            dic[str(i).lower()] = dic[str(i).lower()].T.dropna()
            dic[str(i).lower()] = dic[str(i).lower()].T
            if str(i).upper() in cab:
                # igual o numero de colunas com o numero de listas
                while len(cab[str(i).upper()]) < dic[str(i).lower()].shape[1]:
                    cab[str(i).upper()].append('col-' + str(len(cab[str(i).upper()])))
                if len(cab[str(i).upper()]) == dic[str(i).lower()].shape[1]:
                    dic[str(i).lower()].columns = cab[str(i).upper()]
                else:
                    dic[str(i).lower()].columns = cab[str(i).upper()][0:dic[str(i).lower()].shape[1]]
    return dic


def tratativa_coleta(dic):
    '''
    montagem de list para incremento no df
    ---------------------------------------
    inserir dicionario de dataframes
    retorna dataframe a170 com cnpj
    '''
    # coleta de dados complementares
    if ('a170' in dic) and ('a100' in dic) and (dic['a170'].shape[0] > 0):
        cod_cli = []
        for i in dic['a170'].index:
            # montar planilha a170 com cnpj
            # coleta index
            index_col = dic['a100'][dic['a100'].index < i].index
            # coleta dados na 0150
            cod_cli.append([index_col[index_col.size - 1], i])
        # fazer o join pelo index
        df_temp = pd.DataFrame(cod_cli, columns=['index_a100', 'index_a170'])
        # coleta os dados das planilhas A100 e A170 em uma.
        df_temp = df_temp.merge(right=dic['a100'][['COD_PART', 'NUM_DOC']], right_index=True,
                                left_on='index_a100').merge(left_on='index_a170', right=dic['a170'].iloc[:, 1:],
                                                            right_index=True)
        dic['a_cem'] = df_temp.merge(right=dic['0150'][['COD_PART', 'NOME', 'CNPJ', 'CPF']], left_on='COD_PART',
                                     right_on='COD_PART')

    # coleta de dados complementares
    if ('c170' in dic) and ('c100' in dic) and (dic['a170'].shape[0] > 0):
        cod_cli = []
        for i in dic['c170'].index:
            # montar planilha a170 com cnpj
            # coleta index
            index_col = dic['c100'][dic['c100'].index < i].index
            # coleta dados na 0150
            cod_cli.append([index_col[index_col.size - 1], i])
        # fazer o join pelo index
        df_temp = pd.DataFrame(cod_cli, columns=['index_c100', 'index_c170'])
        # coleta os dados das planilhas A100 e A170 em uma.
        df_temp = df_temp.merge(right=dic['c100'][['COD_PART', 'NUM_DOC']], right_index=True,
                                left_on='index_c100').merge(left_on='index_c170', right=dic['c170'].iloc[:, 1:],
                                                            right_index=True)
        dic['c_cem'] = df_temp.merge(right=dic['0150'][['COD_PART', 'NOME', 'CNPJ', 'CPF']], left_on='COD_PART',
                                     right_on='COD_PART')
    # f600 ja tem o CNPJ

    if ('c190' in dic) and (dic['c190'].shape[0] > 0):
        dic['c190'] = dic['c190'].merge(right=dic['0150'][['COD_PART', 'NOME', 'CNPJ', 'CPF']], left_on='COD_PART',
                                        right_on='COD_PART')
    if ('c395' in dic) and (dic['c395'].shape[0] > 0):
        dic['c395'] = dic['c395'].merge(right=dic['0150'][['COD_PART', 'NOME', 'CNPJ', 'CPF']], left_on='COD_PART',
                                        right_on='COD_PART')
    if ('c500' in dic) and (dic['c500'].shape[0] > 0):
        dic['c500'] = dic['c500'].merge(right=dic['0150'][['COD_PART', 'NOME', 'CNPJ', 'CPF']], left_on='COD_PART',
                                        right_on='COD_PART')
    if ('d100' in dic) and (dic['d100'].shape[0] > 0):
        dic['d100'] = dic['d100'].merge(right=dic['0150'][['COD_PART', 'NOME', 'CNPJ', 'CPF']], left_on='COD_PART',
                                        right_on='COD_PART')
    if ('d500' in dic) and (dic['d500'].shape[0] > 0):
        dic['d500'] = dic['d500'].merge(right=dic['0150'][['COD_PART', 'NOME', 'CNPJ', 'CPF']], left_on='COD_PART',
                                        right_on='COD_PART')
    return dic


def planilha_fat_cred(dic):
    '''
    função para tratmaneto de data frame para inserção na planilha final
    Parameters
    ----------
    dic : TYPE - dictionary

    Returns - Dictionary
    -------
    '''
    if ('a_cem' in dic) and (dic['a_cem'].shape[0] > 0):
        # coleta dados faturamento
        dic['faturamento'] = \
        dic['a_cem'][['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'DESCR_COMPL', 'VL_ITEM', 'CST_PIS', 'VL_PIS', 'VL_COFINS']][
            dic['a_cem']['CST_PIS'] == '01']
        # coleta dados credito
        df_temp = \
        dic['a_cem'][['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'DESCR_COMPL', 'VL_ITEM', 'CST_PIS', 'VL_PIS', 'VL_COFINS']][
            dic['a_cem']['CST_PIS'] != '01']
        if ('credito' in dic):
            df_temp = pd.concat([df_temp, dic['credito']])
            dic['credito'] = df
        dic['credito'] = df_temp
    if ('c_cem' in dic) and (dic['c_cem'].shape[0] > 0):
        # coleta dados credito
        df_temp = dic['c_cem'][
            ['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'DESCR_COMPL', 'VL_ITEM', 'CST_PIS', 'VL_PIS', 'VL_COFINS']]
        if ('credito' in dic):
            df_temp = pd.concat([df_temp, dic['credito']])
            dic['credito'] = df
        dic['credito'] = df_temp
    if ('c190' in dic) and (dic['c190'].shape[0] > 0):
        # coleta dados credito
        df_temp = dic['c190'][['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'VL_DOC', 'VL_PIS', 'VL_COFINS']]
        df_temp.rename(columns={'VL_DOC': 'VL_ITEM'}, inplace=True)
        if ('credito' in dic):
            df_temp = pd.concat([df_temp, dic['credito']])
        dic['credito'] = df_temp
    if ('c395' in dic) and (dic['c395'].shape[0] > 0):
        # coleta dados credito
        df_temp = dic['c395'][['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'VL_DOC']]  # ,'VL_PIS','VL_COFINS']]
        df_temp.rename(columns={'VL_DOC': 'VL_ITEM'}, inplace=True)
        if ('credito' in dic):
            df_temp = pd.concat([df_temp, dic['credito']])
        dic['credito'] = df_temp
    if ('c500' in dic) and (dic['c500'].shape[0] > 0):
        # coleta dados credito
        df_temp = dic['c500'][['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'VL_DOC', 'VL_PIS', 'VL_COFINS']]
        df_temp.rename(columns={'VL_DOC': 'VL_ITEM'}, inplace=True)
        if ('credito' in dic):
            df_temp = pd.concat([df_temp, dic['credito']])
        dic['credito'] = df_temp

    if ('d100' in dic) and (dic['d100'].shape[0] > 0):
        # coleta dados credito d100
        df_temp = dic['d100'][['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'VL_DOC']]  # ,'VL_PIS','VL_COFINS']]
        df_temp.rename(columns={'VL_DOC': 'VL_ITEM'}, inplace=True)
        if ('credito' in dic):
            df_temp = pd.concat([df_temp, dic['credito']])
        dic['credito'] = df_temp
    if ('d500' in dic) and (dic['d500'].shape[0] > 0):
        # coleta dados credito
        df_temp = dic['d500'][['NOME', 'CNPJ', 'CPF', 'NUM_DOC', 'VL_DOC', 'VL_PIS', 'VL_COFINS']]
        df_temp.rename(columns={'VL_DOC': 'VL_ITEM'}, inplace=True)
        if ('credito' in dic):
            df_temp = pd.concat([df_temp, dic['credito']])
        dic['credito'] = df_temp
    # alterar string das colunas para conversão
    dic['faturamento'][['VL_ITEM', 'CST_PIS', 'VL_PIS', 'VL_COFINS']] = dic['faturamento'][
        ['VL_ITEM', 'CST_PIS', 'VL_PIS', 'VL_COFINS']].replace(',', '.', regex=True)
    dic['credito'][['VL_ITEM', 'CST_PIS', 'VL_PIS', 'VL_COFINS']] = dic['credito'][
        ['VL_ITEM', 'CST_PIS', 'VL_PIS', 'VL_COFINS']].replace(',', '.', regex=True)
    # alterar tipo das colunas
    dic['faturamento'] = dic['faturamento'].astype(
        {'NOME': str, 'CNPJ': str, 'CPF': str, 'NUM_DOC': str, 'DESCR_COMPL': str, 'VL_ITEM': float, 'CST_PIS': float,
         'VL_PIS': float, 'VL_COFINS': float})
    dic['credito'] = dic['credito'].astype(
        {'NOME': str, 'CNPJ': str, 'CPF': str, 'NUM_DOC': str, 'DESCR_COMPL': str, 'VL_ITEM': float, 'CST_PIS': float,
         'VL_PIS': float, 'VL_COFINS': float})
    return dic


# path = str(sys.argv[1])
# path_saida = str(sys.argv[2])
'''
coleta de dados dos arquivos 
'''
# abrir arquivos
path = 'C:/Users/Administrador/Desktop/fluxo de caixa/EFD CONTRIBUIÇÃO 2022'
path_saida = r'C:\Users\Administrador\Desktop\fluxo de caixa\planilhas_div\teste_2024'

for file_name in os.listdir(path):
    dic = {}
    df = pd.DataFrame()
    lst_txt = []
    if os.path.isfile(path + '\\' + file_name) and file_name.find('txt') >= 0:
        # abrir aquivo
        file_path = path + '\\' + file_name
        # leitura do arquivo
        file = open(file_path, mode='r', encoding='ansi')
        # salvar varaivel
        txt = file.readlines()
        # fechar arquivo
        file.close()
        # lst_txt = []
        for ln in txt:
            lst_txt.append(ln.split('|'))
        # Tratar e organizar dados
        dic = criar_df(lst_txt)
        # Tratar planilhas
        dic = tratativa_coleta(dic)
        # criar planilha de faturamento
        dic_final = planilha_fat_cred(dic)
        # criação de planilha
        if os.path.isfile(path_saida + '\\' + file_name.replace('txt', 'xlsx')): os.remove(
            path_saida + '\\' + file_name.replace('txt', 'xlsx'))
        with pd.ExcelWriter(path_saida + '\\' + file_name.replace('txt', 'xlsx'), mode='w') as wr:
            for i in dic_final:
                dic_final[i].to_excel(wr, sheet_name=i, index=False)

