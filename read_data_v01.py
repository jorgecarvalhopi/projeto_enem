import os
import time
import pandas as pd
import pyarrow.csv
import pyarrow as pa

def read_parquet(path):
    files = os.listdir(path)

    for file in files:
        df = po.DataFrame()
        df_temp = po.read_parquet(path + file)
        df = po.concat([df_temp, df], how="diagonal")
    return df

def convert_csv_to_parquet(read_path, save_path):
    files = os.listdir(path)

    def rename_columns(dataframe):
        try:
            dataframe = dataframe.rename({
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
        
    def read_csv(path, file, name_to_save, separator):
        print(f'Reading {path + file}')
    
        #Reading csv file
        mem_pool = pa.default_memory_pool()
        read_options = pyarrow.csv.ReadOptions(block_size = 1024**2, encoding = 'iso8859-1')
        parse_options = pyarrow.csv.ParseOptions(delimiter = separator)

        table = pyarrow.csv.read_csv(path + file,
                                     read_options = read_options,
                                     parse_options = parse_options,
                                     memory_pool = mem_pool)
        print(f'Allocated Memory: {round(mem_pool.max_memory()/1024**3, 2)}GB')

        df_temp = table.to_pandas()
        
        #Call customn rename function
        df_temp = rename_columns(df_temp)

        #Creating a parquet file 
        df_temp.to_parquet(save_path + name_to_save)

        #Return shape
        return df_temp.shape
    
    for file in files:
        name_to_save = file.strip('.csv')

        try:
            file_shape = read_csv(read_path, file, name_to_save, ';')

            if file_shape[1] == 1:
                file_shape = read_csv(read_path, file, name_to_save, '|')
        except:
            file_shape = read_csv(read_path, file, name_to_save, '|')

        print(f'File was successful converted \nSaved path: {save_path + name_to_save} \nWith shape of {file_shape}\n')

    return print(f'The convertion process taked {time.process_time()/60} minutes')
        
if __name__ == '__main__':
    #Paths
    path = '/home/jorge/Documents/Projeto_ENEM/chamada_regular/'
    save_path_parquet = '/home/jorge/Documents/Projeto_ENEM/chamada_regular_parquet/'
    save_path_utf = '/home/jorge/Documents/Projeto_ENEM/chamada_regular_utf8/'

    convert_csv_to_parquet(path, save_path_parquet)