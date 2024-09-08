import logging
import os

def find_path(level: int = 0) -> str:

    if level == 0:
        current_directory = os.getcwd()
        return current_directory

    # Obtém o diretório atual
    current_directory = os.getcwd()

    for _ in range(0, level):
        # Obtém o caminho absoluto do diretório pai
        current_directory = os.path.abspath(os.path.join(current_directory, os.pardir))

    return current_directory


def setup_custom_logger(
    logfile: str = os.getcwd(),
    level: int = logging.INFO
) -> logging.Logger:

    # Extrai o diretório do caminho do logfile
    log_directory = os.path.dirname(logfile)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Configuração do logger com codificação UTF-8
    logger = logging.getLogger('custom_logger')                                 #? Criando uma instância de logger, ou seja, o objeto
    logger.setLevel(level)                                                      #? Definindo o nível de mensagens que serão registradas
    file_handler = logging.FileHandler(logfile, encoding='utf-8')               #? Define o manipulador de arquivo, ele quem registrará
    file_handler.setLevel(level)                                                #? Definindo o nível de mensagens que serão processadas
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  #? Define o formato de mensagens
    file_handler.setFormatter(formatter)                                        #? Define o formato de mensagens para o manipulador
    logger.addHandler(file_handler)                                             #? Adiciona o manipulador de arquivo ao logger

    return logger
