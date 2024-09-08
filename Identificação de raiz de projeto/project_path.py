import os
import sys

def find_project_root(search_patterns: list, max_depth: int=10):
    """
    Encontra a raiz do projeto procurando por pastas ou arquivos comuns
    como src, .env, .gitignore, etc.
    
    search_patterns: Lista de nomes de pastas ou arquivos que indicam a raiz do projeto.
    max_depth: Limite de quantos diretórios acima deve procurar para evitar loops infinitos.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    depth = 0
    
    while current_dir and depth < max_depth:
        if all(os.path.exists(os.path.join(current_dir, pattern)) for pattern in search_patterns):
            project_root = current_dir
            sys.path.append(project_root)
            return project_root
        
        
        # Vai para o diretório pai
        parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
        
        if parent_dir == current_dir:  # Chegou à raiz do sistema
            break
        
        current_dir = parent_dir
        depth += 1
    
    return None  # Não encontrou a raiz do projeto