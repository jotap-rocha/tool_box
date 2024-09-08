from utils import database
import pymsteams
import warnings
warnings.filterwarnings('ignore')

class MasterMold:
    
    def __init__(self, load, teams_channel):
        
        self.load = load
        self.teams_channel = teams_channel
        self.outdated_time_minutes = None
        self.outdated_time_hour = None
        self.outdated_time_day = None


    def check_load(self): # Método que irá verificar a atualização do banco

        # Conectar no banco
        diff_seconds = database.consult_database(self.load.database, self.load.scheema, self.load.table, self.load.target_column)

        self.outdated_time_day, rest = divmod(diff_seconds, 86400)   # Divide segundos por 86400 para obter os dias
        self.outdated_time_hour, rest = divmod(rest, 3600)           # Divide o resto por 3600 para obter as horas
        self.outdated_time_minutes, _ = divmod(rest, 60)             # Divide o resto por 60 para obter os minutos

        if diff_seconds > self.load.threshold:
            return False

        else:
            return True


    def send_notifications(self):
        
        myTeamsMessage = pymsteams.connectorcard(self.teams_channel, verify=False)
        
        day = self.outdated_time_day
        hour = self.outdated_time_hour
        minute = self.outdated_time_minutes
        
        message = f'ATENÇÃO, TIME!'
        message += f'<br><br>A carga {self.load.name} está há {day} dia(s), {hour} hora(s) e {minute} minuto(s) sem chegar ao banco {self.load.database} do IHMTZBDBI.'
        message += f'<br><br>Por favor, chequem o ETL.'
        message += f'<br><br>Servidor: IHMTZBDBI<br>Banco: {self.load.database}<br>Tabela: {self.load.table}'
        message += f'<br><br>Link da documentação de referência para resolução do problema -> {self.load.documentation_link}'
        
        myTeamsMessage.text(message)
        
        myTeamsMessage.send()
