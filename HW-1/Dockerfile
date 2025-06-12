FROM mysql:8.4

# 1. curl потрібен 01_download_data.sh
RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs \
        install -y curl python3 python3-pip \
    && pip3 install --no-cache-dir gdown \
    && microdnf clean all

# 2. Дозволимо mysql-користувачеві писати CSV
RUN mkdir -p /docker-entrypoint-initdb.d/data \
    && chown -R mysql:mysql /docker-entrypoint-initdb.d/data

# 3. Конфіг, що вмикає local_infile
COPY config/local_infile.cnf /etc/my.cnf.d/

# 4. Init-скрипти (SQL + SH)
COPY init/ /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/*.sh
