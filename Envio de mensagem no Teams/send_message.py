import pymsteams

myTeamsMessage = pymsteams.connectorcard(teams_channel, verify=False)

day = self.outdated_time_day
hour = self.outdated_time_hour
minute = self.outdated_time_minutes

message = f'ATENÇÃO, TIME!'
message += f'<br><br>A carga {name} está há {day} dia(s), {hour} hora(s) e {minute} minuto(s) sem chegar ao banco {database} do IHMTZBDBI.'
message += f'<br><br>Por favor, chequem o ETL.'
message += f'<br><br>Servidor: {server}<br>Banco: {database}<br>Tabela: {table}'
message += f'<br><br>Link da documentação de referência para resolução do problema -> {documentation_link}'

myTeamsMessage.text(message)

myTeamsMessage.send()
