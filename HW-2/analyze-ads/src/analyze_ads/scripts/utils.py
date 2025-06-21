from pathlib import Path

import pandas as pd

from analyze_ads.db import get_connection

BASE_DIR = Path(__file__).resolve().parent.parent
QUERIES_DIR = BASE_DIR / "queries"
REPORTS_DIR = BASE_DIR / "reports"


def fetch_data_from_query(query_filename: str) -> pd.DataFrame:
    """
    Читає SQL-запит з файлу, виконує його та повертає результат у вигляді DataFrame.

    Args:
        query_filename (str): Назва SQL файлу в папці 'queries'.

    Returns:
        pd.DataFrame: DataFrame з результатами запиту.
    """
    query_path = QUERIES_DIR / query_filename

    with open(query_path, "r") as file:
        query = file.read()

    # Використання 'with' гарантує, що з'єднання буде закрито автоматично,
    # навіть якщо виникне помилка.
    with get_connection() as conn:
        df = pd.read_sql(query, conn)

    return df


def save_report(df: pd.DataFrame, filename_base: str):
    """
    Зберігає DataFrame у форматах CSV та JSON у теці 'reports'.

    Args:
        df (pd.DataFrame): DataFrame для збереження.
        filename_base (str): Базове ім'я файлу (без розширення).
    """
    # Переконуємось, що папка для звітів існує
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = REPORTS_DIR / f"{filename_base}.csv"
    json_path = REPORTS_DIR / f"{filename_base}.json"

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=4)
    print(f"Звіт '{filename_base}' успішно збережено в CSV та JSON.")
