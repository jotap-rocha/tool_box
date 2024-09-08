
class Load:
    
    def __init__(
        self,
        database: str,
        scheema: str,
        table: str,
        target_column: str,
        threshold: str,
        name: str = "Carga Aleatória",
        documentation_link: str = "Não informado"
    ):
        
        self.name = name # Nome da carga
        self.documentation_link = documentation_link # Link da documentação de referência
        self.database = database # Banco de dados onde a carga está sendo feita
        self.scheema = scheema # Scheema da tabela onde a carga é feita
        self.table = table # Tabela destino, onde a verificação deve ser realizada
        self.target_column = target_column # Coluna controladora do fluxo de carga
        self.threshold = threshold # Tempo máximo, em segundos, que a tabela pode ficar sem atualização
