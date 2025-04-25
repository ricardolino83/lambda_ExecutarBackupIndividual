# Usa a imagem base Red Hat Universal Base Image 9
FROM redhat/ubi9:latest

# Define o diretório de trabalho
WORKDIR /app

# Instala Python 3.9, pip e dependências essenciais
# RHEL 9 UBI geralmente vem com Python 3.9, mas garantimos a instalação e o pip.
# Adiciona ferramentas necessárias pelo script e pela instalação do pyodbc.
RUN yum update -y && \
    yum install -y --allowerasing python3 python3-pip python3-devel gcc-c++ curl gnupg unixODBC-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# Script fornecido pelo usuário para instalar o MS ODBC Driver 18
# (Adaptado para execução no Dockerfile)
# Nota: A checagem de versão RHEL 8/9 é inerentemente satisfeita pela imagem base ubi9.
#       Executando como root no build, 'sudo' não é necessário.
RUN curl -sSL -O https://packages.microsoft.com/config/rhel/9/packages-microsoft-prod.rpm && \
    rpm -i packages-microsoft-prod.rpm && \
    rm packages-microsoft-prod.rpm && \
    # Remove conflitos potenciais, se existirem (ignora erros se não existirem)
    yum remove -y unixODBC-utf16 unixODBC-utf16-devel || true && \
    # Instala o driver ODBC e ferramentas (aceita EULA)
    ACCEPT_EULA=Y yum install -y msodbcsql18 mssql-tools18 && \
    yum clean all && \
    rm -rf /var/cache/yum

# Adiciona as ferramentas da Microsoft ao PATH (para o ambiente do container)
# É melhor definir ENV para persistir no container
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# Copia o arquivo de requisitos
COPY requirements.txt ./

# Instala as dependências Python
# Usar python3.9 e pip3.9 explicitamente
# Instala as dependências Python, atualizando o pip primeiro
# Instala as dependências Python, atualizando o pip primeiro e usando o índice explícito
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir --index-url https://pypi.org/simple/ -r requirements.txt

# Copia o código da função Lambda (assumindo o mesmo nome do original)
COPY lambda_function.py ./

# Define o comando para executar a função Lambda
# O formato é <nome_do_arquivo>.<nome_da_função_handler>
CMD [ "python3", "-m", "awslambdaric", "lambda_function.lambda_handler" ]
