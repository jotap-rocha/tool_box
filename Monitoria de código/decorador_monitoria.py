import functools


def log_exceptions():
    """
    Decorador genérico para captura de exceções e tratamento de erros.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            job_context = kwargs.get('job_context')  # Obtemos o objeto JobContext

            try:
                # Executa a função decorada
                result = func(*args, **kwargs)
                return result
            except Exception as error:
                # Extraímos as variáveis diretamente do job_context
                # Aqui você atribui as variáveis que serão necessárias durante a monitoria do seu código
                logger = job_context.logger
                spark = job_context.spark
                job_name = job_context.job_name
                process = job_context.process
                project = job_context.project
                environment = job_context.environment
                error_function = func.__name__  # Captura o nome da função com erro

                # Mensagem de erro personalizada que foi passada como keyword argument
                message = kwargs.get('message', 'Erro sem mensagem')


        return wrapper
    return decorator