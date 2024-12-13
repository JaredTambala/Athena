FROM ghcr.io/mlflow/mlflow:v2.15.0
COPY ./mlflow/mlflow_requirements.txt /

# Install basic build
RUN apt-get update
RUN apt-get install -y --no-install-recommends make build-essential libssl-dev zlib1g-dev && \
    apt-get install -y libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev && \
    apt-get install -y xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

# Install git
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y gcc && \
    apt-get install -y git

# Install dependencies
RUN apt-get -y update && \
    apt-get -y install python3-dev default-libmysqlclient-dev build-essential pkg-config && \
    apt-get -y install python3-pymysql && \
    pip install --upgrade pip && \
    pip install -r mlflow_requirements.txt

# Install pyenv
ENV HOME="/root"
WORKDIR ${HOME}
RUN apt-get install -y git
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

ENV PYTHON_VERSION=3.10.4
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}
