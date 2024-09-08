import os
import sys

# Retornando dois níveis de pasta para acessar o pacote setup
current_dir = os.path.dirname(os.path.abspath(__file__))
src_folder = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.append(src_folder)

# Adicionando a raiz do projeto ao sys.path, para que seja possível importar arquivos auxiliares
from project_path import find_project_root
project_root = find_project_root(["src"])
os.chdir(project_root)