# tool_box
Repositório python com códigos focados em utilidades, aqui você encontrará códigos úteis para projetos de automação, engenharia, monitoria de código, etc. Vale lembrar que este repositório estará em constante atualização.

# Estrutura do repositório

- /Banco de dados: Se você deseja integrar seus scripts python com um banco de dados SQL Server, esta pasta te ajudará bastante. O pacote de códigos conta com funções para ingestão em bancos, consultas de leitura, geração de massa de informação e truncate em tabelas.

- /Envio de mensagem no Teams: Nesta pasta você encontrará um pacote de códigos focados no envio de uma mensagem para o canal de equipes do Microsoft Teams. Deixei uma dica dentro do repositório de como potencializar a dinâmica de envio de mensagem no Teams para se adaptar melhor à tarefas corporativas.

- /Identificação de raiz de projeto: Esta pasta é recomendada para aqueles desenvolvedores um pouco mais experientes que organizam a estrutura de pastas dos projetos nos quais trabalham. Como boa prática, sempre é adicionado a estrutura do projeto uma pasta de ambiente virtual, documentação, logs e uma pasta chamada src (source). A pasta src possui o objetivo de armazenar o código principal, classes e pacotes que auxiliarão seu script principal a executar.

- /Monitorador de cargas: O nome é sugestivo, é uma classe que monitora ingestões feitas num banco SQL Server, com o intuito de auxiliar o time de dados a verificar, de forma automática, a execução automática dos jobs. É muito degradante a equipe realizar essa verificação manualmente.

- /Monitoria de código: À medida que muitos jobs de dados ou aplicações são construídas, é difícil garantirmos a integridade dos scripts, quando executou, se executou com sucesso, onde ocorreu a falha, que falha ocorreu. A monitoria serve justamente para facilitar este trabalho. Destinada para desenvolvedores mais experientes, este pacote de códigos vai auxiliar a observabilidade de seu script, registrando o log num arquivo ".log" e integrando com um banco de dados.

- / Normalização de JSON: Diretório focado para engenharia, este possui uma classe que realiza normalizações automáticas de JSON's presente em linhas. Há muita dificuldade em lidar com JSON's em formato de linha dentro de uma tabela, mas a factory te ajudará nesta tarefa.

# Como utilizar o repositório

Você pode clonar o repositório digitando o seguinte comando:

```
git clone https://github.com/jotap-rocha/tool_box.git
```

```
cd tool_box
```

Não esqueça de instalar as dependências do arquivo "requirements.txt".

# Casos de uso

Dentro de cada pasta haverá um exemplo de caso de uso exemplificando como você poderá usar e adotar para o seu problema

# Contribuições

Contribuições são bem-vindas! Por favor, abra um pull request ou reporte problemas na aba de issues.

# Contato

Para mais informações ou retirada de dúvidas, deixo meu email para contato: jp.rocha2020@gmail.com