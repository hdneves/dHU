import os
import logging
import shutil
from constants import DIR_DELETE

def configure_logging() -> logging:
 
    logging.basicConfig(filename='LOG-records_ofCNES.log', level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger(__name__)
    return logger

def create_folder(
        folder_name = 'downloadCnes'
    ) -> None:
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    print('pasta criada')

def delete_files(
        dir_delete: str = DIR_DELETE
) -> None:

    # for folder in os.listdir(download_dir):
    #     folder_path = os.path.join(download_dir, folder)
    #     shutil.rmtree(folder_path)

    shutil.rmtree(dir_delete)