#!/bin/bash
#
# Створює резервну копію бази даних, виключаючи вказані "сирі" таблиці.
#
set -e

# Переконуємося, що ми знаходимось у каталозі db/
cd "$(dirname "$0")"

CONTAINER_NAME="adtech-mysql"
BACKUP_DIR="./backup"

# Перевіряємо, чи запущений контейнер
if [ ! "$(docker ps -q -f name=^/${CONTAINER_NAME}$)" ]; then
    echo "Error: Container ${CONTAINER_NAME} is not running."
    echo "Please start the database first."
    exit 1
fi

# Створюємо каталог для бекапів, якщо його немає
mkdir -p "$BACKUP_DIR"

BACKUP_FILENAME="backup_$(date +%Y%m%d_%H%M%S)_no_raw.sql.gz"
BACKUP_PATH="${BACKUP_DIR}/${BACKUP_FILENAME}"

DB_NAME="${MYSQL_DATABASE:-AdTech}"

echo "▶ Creating database backup for '${DB_NAME}'..."
echo "  Ignoring tables: RawEvents, RawUsers, RawCampaigns"

# команда mysqldump з прапорами --ignore-table
# Важливо вказувати назву бази даних перед назвою таблиці (DB_NAME.TABLE_NAME)
docker exec "$CONTAINER_NAME" sh -c \
    "mysqldump -u root -p'${MYSQL_ROOT_PASSWORD:-rootpass}' \
    --ignore-table=${DB_NAME}.RawEvents \
    --ignore-table=${DB_NAME}.RawUsers \
    --ignore-table=${DB_NAME}.RawCampaigns \
    ${DB_NAME} | gzip" > "$BACKUP_PATH"

echo "✓ Backup created successfully: ${BACKUP_PATH}"
