FROM mysql:8.4

# 1.
# Встановлюємо утиліти:
     ## - python3-pip для встановлення gdown
     ## - gdown для завантаження файлів з Google Drive
     ## - gzip для роботи зі стиснутими архівами
## Використовуємо dnf, оскільки він є стандартним для базового образу MySQL (Oracle Linux)
RUN microdnf install -y python3-pip gzip && \
    pip3 install --no-cache-dir gdown && \
    microdnf clean all

# 2. Дозволимо mysql-користувачеві писати CSV
RUN mkdir -p /docker-entrypoint-initdb.d/data \
    && chown -R mysql:mysql /docker-entrypoint-initdb.d/data

# 3. Конфіг, що вмикає local_infile
COPY config/local_infile.cnf /etc/my.cnf.d/

# 4. Init-скрипти (SQL + SH)
COPY init/ /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/*.sh
