#!/bin/bash
set -euo pipefail

TARGET_DIR="/var/lib/mysql-files"
echo "⬇️  Завантажую у $TARGET_DIR"

human_size() { du -h "$1" | awk '{print $1}'; }

gget () {   # $1 = Drive file ID,  $2 = локальне ім'я
  local id="$1" name="$2"
  printf "\n📄 %s\n" "$name"
  gdown --id "$id" --output "$TARGET_DIR/$name" --no-cookies --fuzzy --quiet
  printf "✅  %s — %s\n" "$name" "$(human_size "$TARGET_DIR/$name")"
}

gget "1B7GmvhSeLA8rot3-_0mBCE1bxhyFZ65L" "events.csv"      # ~1.9 GB
gget "18gEty-UqAd0UkuVwL4V2rdTCDaBK8406" "users.csv"
gget "1I54DEoDfPohEPbLmHWWW-iED8MsdFeId" "campaigns.csv"

echo -e "\n🎉  Усі файли в $TARGET_DIR"
