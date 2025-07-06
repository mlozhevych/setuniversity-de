import json
import os

import pymongo
from analyze_ads_nosql.mongo_queries.ad_fatigue import get_query_4
from analyze_ads_nosql.mongo_queries.ad_interactions import get_query_1
from analyze_ads_nosql.mongo_queries.clicks_per_hour import execute_query_3
from analyze_ads_nosql.mongo_queries.last_sessions import get_query_2
from analyze_ads_nosql.mongo_queries.top_categories import get_query_5
from analyze_ads_nosql.utils import build_mongo_uri, get_db_connection, save_results
from dotenv import load_dotenv
from rich.console import Console
from rich.prompt import Prompt, IntPrompt

load_dotenv()
console = Console()

# --- Головне меню ---


def main():
    """Головна функція, що відображає меню та керує процесом."""
    uri = build_mongo_uri()
    db_name = os.getenv("MONGO_DB", "AdTech")
    client, db = get_db_connection(uri, db_name)
    if db is None:
        return

    sessions_collection = db.sessions

    query_map = {
        "1": ("Отримати всі рекламні взаємодії для конкретного користувача", get_query_1, True),
        "2": ("Отримати останні 5 рекламних сесій користувача", get_query_2, True),
        "3": ("Кількість кліків за годину для кампаній (Advertiser_82)", execute_query_3, False),
        "4": ("Виявлення 'втоми від реклами'", get_query_4, False),
        "5": ("Топ-3 категорії за кліками для користувача", get_query_5, True),
    }

    while True:
        console.print("\n[bold magenta]--- Меню запитів MongoDB ---[/bold magenta]")
        for key, (desc, _, _) in query_map.items():
            console.print(f"[cyan]{key}[/cyan]: {desc}")
        console.print("[cyan]q[/cyan]: Вийти")

        choice = Prompt.ask("Виберіть номер запиту", choices=list(query_map.keys()) + ["q"])

        if choice == 'q':
            console.print("[bold]До побачення![/bold]")
            break

        description, query_func, requires_uid = query_map[choice]
        console.print(f"\nВи обрали: [bold yellow]{description}[/bold yellow]")

        results = []
        pipeline = None

        try:
            if choice == "3":  # Особливий випадок
                advertiser_name = Prompt.ask("Введіть ім'я рекламодавця", default="Advertiser_82")
                results = query_func(sessions_collection, advertiser_name)
            else:
                if requires_uid:
                    user_id = IntPrompt.ask("Введіть ID користувача (напр., 10)")
                    pipeline = query_func(user_id)
                else:
                    pipeline = query_func()

                console.print("...Виконується запит...")
                results = list(sessions_collection.aggregate(pipeline))

            console.print(f"Знайдено [bold cyan]{len(results)}[/bold cyan] результатів.")

            if results:
                # Показати перші 3 результати для попереднього перегляду
                console.print("\n[bold]Попередній перегляд результатів:[/bold]")
                console.print(json.dumps(results[:3], indent=2, default=str))

                # Збереження файлу
                save_choice = Prompt.ask("\nЗберегти результати у файл?", choices=["y", "n"], default="y")
                if save_choice == 'y':
                    file_format = Prompt.ask("Виберіть формат", choices=["json", "csv"], default="json")
                    default_filename = f"query_{choice}_results.{file_format}"
                    filename = Prompt.ask("Введіть ім'я файлу", default=default_filename)
                    save_results(results, filename, file_format)

        except pymongo.errors.PyMongoError as e:
            console.print(f"[bold red]Помилка виконання запиту до MongoDB:[/bold red] {e}")
        except Exception as e:
            console.print(f"[bold red]Сталася неочікувана помилка:[/bold red] {e}")


if __name__ == "__main__":
    main()
