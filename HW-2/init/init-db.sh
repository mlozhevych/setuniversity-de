#!/bin/bash
#
# Цей скрипт автоматично завантажує та ВІДНОВЛЮЄ базу даних з Google Drive,
# використовуючи оптимізації для прискорення імпорту великих файлів.
#
set -e

# ID файлу з вашого посилання на Google Drive
GDRIVE_FILE_ID="1wkqEfxzwe5zNU4JyDXHz0KR0JG_UQy31"
BACKUP_FILE="/tmp/backup.sql.gz"
OPTIMIZED_SQL_FILE="/tmp/optimized_restore.sql"

echo "▶ Downloading database backup from Google Drive..."
if gdown --id "$GDRIVE_FILE_ID" -O "$BACKUP_FILE"; then
    echo "✓ Backup downloaded successfully."
else
    echo "✗ Failed to download backup. Please check the file ID and permissions."
    exit 1
fi

echo "▶ Restoring database from backup (with optimizations)..."

# Створюємо тимчасовий SQL-файл, куди додамо оптимізаційні команди.
# Це стандартна практика для прискорення великих імпортів.

# На початку файлу вимикаємо перевірки, які сповільнюють імпорт.
echo "SET autocommit=0;" > "$OPTIMIZED_SQL_FILE"
echo "SET unique_checks=0;" >> "$OPTIMIZED_SQL_FILE"
echo "SET foreign_key_checks=0;" >> "$OPTIMIZED_SQL_FILE"

# Розпаковуємо бекап і додаємо його вміст до нашого файлу
gunzip -c "$BACKUP_FILE" >> "$OPTIMIZED_SQL_FILE"

# В кінці файлу повертаємо всі перевірки та зберігаємо зміни
echo "SET foreign_key_checks=1;" >> "$OPTIMIZED_SQL_FILE"
echo "SET unique_checks=1;" >> "$OPTIMIZED_SQL_FILE"
echo "COMMIT;" >> "$OPTIMIZED_SQL_FILE"


# Виконуємо весь оптимізований SQL-файл за один раз
mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" < "$OPTIMIZED_SQL_FILE"

echo "✓ Database restored successfully into '${MYSQL_DATABASE}'."

# Видаляємо тимчасові файли
rm "$BACKUP_FILE" "$OPTIMIZED_SQL_FILE"

echo "★ Database setup is complete. MySQL is ready to accept connections."
