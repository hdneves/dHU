import pandas as pd
import pyodbc
import zipfile
import ftplib
import logging
from sqlalchemy import create_engine
from datetime import datetime
from io import BytesIO
from constants import STRING_CONNECTION, DIR_DOWNLOAD, FILE_NAME, QUERY_SQL, UNZIP_FILE, COLUMNS
from utility import configure_logging, create_folder, delete_files


def download_zip(
        logger:logging,
        file_name:str = FILE_NAME,
        dir_download = DIR_DOWNLOAD
    )-> None:

    logger.info(f'Creating folder: {file_name}')
    create_folder()
    logger.info('Folder has been created.')
    ftp_server = ftplib.FTP("ftp.datasus.gov.br")
    ftp_server.login()
    ftp_server.cwd("/cnes/")
    #file_list = ftp_server.nlst() #lista arquivos
    
    logger.info('Downloading files...')
    with open(dir_download, "wb") as local_file:
        ftp_server.retrbinary(f"RETR {file_name}", local_file.write)

    ftp_server.quit()
    logger.info('Download has finished')

def read_zipFile(
        logger:logging,
        dir_download: str = DIR_DOWNLOAD,
        unzip_file: str = UNZIP_FILE
    ) -> BytesIO:
    logger.info('Opening file...')
    with zipfile.ZipFile(dir_download, 'r') as zip_ref:
        with zip_ref.open(unzip_file) as arquivo_zipado:
            conteudo_zipado = arquivo_zipado.read().decode('iso-8859-1')

    bytes_io = BytesIO(conteudo_zipado.encode()) #Função BytesIO tem a função de simular um arquivo aberto

    logger.info(f'The function has opened the file: {unzip_file}')
    return bytes_io

def extract_columns(
        logger: logging,
        bytes_io: BytesIO,
        wish_columns: str = COLUMNS
    ) -> pd.DataFrame:

    logger.info('Reading .csv file')

    dataframe = pd.read_csv(bytes_io, sep=';', dtype=str)
    #dataframe.to_excel(f'backup_{file_name}')
    #dataframe = pd.read_csv('tbEstabelecimento202402.csv', sep=';', encoding='ISO-8859-1')
    dataframe = dataframe[wish_columns]
    dataframe['DATA_INCLUSAO'] = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    logger.info('The necessary columns have been extracted.')
    return dataframe

def data_updateSQL(
        logger: logging,
        dataframe:pd.DataFrame,
        query_del:str = QUERY_SQL,
        string_connection:tuple = STRING_CONNECTION
) -> None:

    logger.info('Connecting with databse')
    conn = pyodbc.connect(string_connection)
    print('conexao ok')
    conn.execute(query_del)
    logger.info('Database has been deleted.')
    conn.commit()

    #Convert the dataframe to a sql table
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(string_connection))
    dataframe.to_sql('CADASTRO_CNES', engine, if_exists='append', index=False)
    logger.info('Databsae has been updated.')

    # Close the connection
    conn.close()

if __name__ == '__main__':
    logger = configure_logging()
    download_zip(logger=logger)
    bytes_io = read_zipFile(logger=logger)
    dataframe = extract_columns(bytes_io=bytes_io, logger=logger)
    data_updateSQL(logger=logger, dataframe=dataframe)
    delete_files()


    #ADICIONAR LOGS
    #FUNÇÃO SQL MESCLADA COM DELETAR E INCLUIR
    #ADICIONAR FUNÇÃO PARA EXCLUIR OS ARQUIVOS BAIXADOS
    #AGENDAEMTNO VAI SER NO SCHEDULE DO WINDOWS

    
    