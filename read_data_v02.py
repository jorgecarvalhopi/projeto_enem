import sqlite3
import os
import time
import pandas as pd
import pyarrow.csv
import pyarrow as pa

def create_tables():
    conn = sqlite3.connect('base.db')
    cursor = conn.cursor()
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS D_INSTITUICOES
        (
            CODIGO_CAMPUS INTEGER PRIMARY KEY,
            NOME_CAMPUS TEXT,
            UF_CAMPUS TEXT,
            MUNICIPIO_CAMPUS TEXT,
            CODIGO_IES INTEGER,
            NOME_IES TEXT,
            SIGLA_IES TEXT,
            UF_IES TEXT
        )
        """
    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()


def insert_data_d_instituicoes(path):
    #Function to rename csv columns, will be call in the READ_CSV function
    def rename_columns(dataframe):
        try:
            dataframe = dataframe.rename(columns = {
                    'ï»¿NU_ANO' : 'ANO',
                    'NU_EDICAO' : 'EDICAO',
                    'CO_ETAPA' : 'ETAPA',
                    'DS_ETAPA' : 'DS_ETAPA',
                    'CO_IES' : 'CODIGO_IES',
                    'NO_IES' : 'NOME_IES',
                    'SIGLA_IES' : 'SIGLA_IES',
                    'SG_UF_IES' : 'UF_IES',
                    'CO_CAMPUS' : 'CODIGO_CAMPUS',
                    'NO_CAMPUS' : 'NOME_CAMPUS',
                    'SG_UF_CAMPUS' : 'UF_CAMPUS',
                    'NO_MUNUCIPIO_CAMPUS' : 'MUNICIPIO_CAMPUS',
                    'CO_IES_CURSO' : 'CODIGO_CURSO',
                    'NO_CURSO' : 'NOME_CURSO',
                    'DS_GRAU' : 'GRAU',
                    'DS_TURNO' : 'TURNO',
                    'TP_MOD_CONCORRENCIA' : 'TIPO_MOD_CONCORRENCIA',
                    'DS_MOD_CONCORRENCIA' : 'MOD_CONCORRENCIA',
                    'NU_PERCENTUAL_BONUS' : 'PERCENTUAL_BONUS',
                    'NU_PESO_L' : 'PESO_L',
                    'NU_PESO_CH' : 'PESO_CH',
                    'NU_PESO_CN' : 'PESO_CN',
                    'NU_PESO_M' : 'PESO_M',
                    'NU_PESO_R' : 'PESO_R',
                    'NOTA_MINIMA_L' : 'NOTA_MINIMA_L',
                    'NOTA_MINIMA_CH' : 'NOTA_MINIMA_CH',
                    'NOTA_MINIMA_CN' : 'NOTA_MINIMA_CN',
                    'NOTA_MINIMA_M' : 'NOTA_MINIMA_M',
                    'NOTA_MINIMA_R' : 'NOTA_MINIMA_R',
                    'MEDIA_MINIMA' : 'MEDIA_MINIMA',
                    'NU_CPF' : 'CPF',
                    'CO_INSCRICAO_ENEM' : 'INSCRICAO_ENEM',
                    'NO_INSCRITO' : 'INSCRITO',
                    'TP_SEXO' : 'SEXO',
                    'DT_NASCIMENTO' : 'DATA_NASCIMENTO',
                    'SG_UF_CANDIDATO' : 'UF_CANDIDATO',
                    'MUNICIPIO_CANDIDATO' : 'MUNICIPIO_CANDIDATO',
                    'ST_OPCAO' : 'OPCAO',
                    'NU_NOTA_L' : 'NOTA_L',
                    'NU_NOTA_CH' : 'NOTA_CH',
                    'NU_NOTA_CN' : 'NOTA_CN',
                    'NU_NOTA_M' : 'NOTA_M',
                    'NU_NOTA_R' : 'NOTA_R',
                    'NOTA_L_COM_PESO' : 'NOTA_L_COM_PESO',
                    'NOTA_CH_COM_PESO' : 'NOTA_CH_COM_PESO',
                    'NOTA_CN_COM_PESO' : 'NOTA_CN_COM_PESO',
                    'NOTA_M_COM_PESO' : 'NOTA_M_COM_PESO',
                    'NOTA_R_COM_PESO' : 'NOTA_R_COM_PESO',
                    'NU_NOTA_CANDIDATO' : 'NOTA_CANDIDATO',
                    'NU_NOTACORTE' : 'NOTA_CORTE',
                    'NU_CLASSIFICACAO' : 'CLASSIFICACAO',
                    'ST_APROVADO' : 'APROVADO',
                    'ST_MATRICULA' : 'MATRICULA'            
                })
            return dataframe
        except:
            return dataframe
        
    #READ_CSV customn function
    def read_csv(path, file, separator):
        print(f'Reading {path + file}')
    
        #Reading csv file with pyarrow
        mem_pool = pa.default_memory_pool()
        read_options = pyarrow.csv.ReadOptions(block_size = 1024**2, encoding = 'iso8859-1')
        parse_options = pyarrow.csv.ParseOptions(delimiter = separator)

        table = pyarrow.csv.read_csv(path + file,
                                     read_options = read_options,
                                     parse_options = parse_options,
                                     memory_pool = mem_pool)
        print(f'Allocated Memory: {round(mem_pool.max_memory()/1024**3, 2)}GB')

        #Convert to Pandas DataFrame
        df_temp = table.to_pandas()
        
        #Call custom rename function
        df_temp = rename_columns(df_temp)

        #Drop columns
        df_temp = df_temp.drop(columns = ['ANO',
                                        'EDICAO',
                                        'ETAPA',
                                        'DS_ETAPA',
                                        'MOD_CONCORRENCIA',
                                        'PERCENTUAL_BONUS',
                                        'PESO_L',
                                        'PESO_CH',
                                        'PESO_CN',
                                        'PESO_M',
                                        'PESO_R',
                                        'NOTA_MINIMA_L',
                                        'NOTA_MINIMA_CH',
                                        'NOTA_MINIMA_CN',
                                        'NOTA_MINIMA_M',
                                        'NOTA_MINIMA_R',
                                        'MEDIA_MINIMA',
                                        'CPF',
                                        'INSCRICAO_ENEM',
                                        'INSCRITO',
                                        'SEXO',
                                        'DATA_NASCIMENTO',
                                        'UF_CANDIDATO',
                                        'MUNICIPIO_CANDIDATO',
                                        'OPCAO',
                                        'NOTA_L',
                                        'NOTA_CH',
                                        'NOTA_CN',
                                        'NOTA_M',
                                        'NOTA_R',
                                        'NOTA_L_COM_PESO',
                                        'NOTA_CH_COM_PESO',
                                        'NOTA_CN_COM_PESO',
                                        'NOTA_M_COM_PESO',
                                        'NOTA_R_COM_PESO',
                                        'NOTA_CANDIDATO',
                                        'NOTA_CORTE',
                                        'CLASSIFICACAO',
                                        'APROVADO',
                                        'MATRICULA'])
        
        #Return shape
        return df_temp, df_temp.shape
    
    #Reading files and insert in the DataBase
    files = os.listdir(path)

    for file in files:
        try:
            df, df_shape = read_csv(path, file, ';')
            if df_shape[1] == 1:
                df, df_shape = read_csv(path, file, '|')
        except:
            df, df_shape = read_csv(path, file, '|')

        df = df.groupby(by=['CODIGO_CAMPUS']).first().reset_index()
        df.to_csv('teste.csv')

    return print(f'The process taked {time.process_time()/60} minutes')


if __name__ == '__main__':
    insert_data_d_instituicoes('/home/jorge/Documents/analise_enem/chamada_regular/')
