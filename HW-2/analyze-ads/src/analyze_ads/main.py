from analyze_ads.scripts.utils import fetch_data_from_query, save_report

# In src/analyze_ads/main.py

REPORTS_CONFIG = {
    "top_5_ctr_campaigns": {
        "query": "campaign_summary.sql",
        "description": "Top-5 кампаній за CTR",
        "solves_task": "Завдання 1: Визначити 5 найкращих кампаній з найвищим показником клікабельності (CTR).",
        "handler": "top_5_ctr"
    },
    "advertiser_summary": {
        "query": "advertiser_summary.sql",
        "description": "Найбільші витрати рекламодавців",
        "solves_task": "Завдання 2: Визначити рекламодавців, які витратили найбільше грошей на покази реклами.",
        "handler": "simple"
    },
    "campaign_summary": {
        "query": "campaign_summary.sql",
        "description": "Зведена статистика по кампаніях (CPC, CPM)",
        "solves_task": "Завдання 3: Розрахувати середню вартість за клік (CPC) та вартість за тисячу показів (CPM) для кожної кампанії.",
        "handler": "simple"
    },
    "location_summary": {
        "query": "location_summary.sql",
        "description": "Найприбутковіші локації",
        "solves_task": "Завдання 4: Знайти найефективніші локації на основі загального доходу, отриманого від кліків.",
        "handler": "simple"
    },
    "user_activity_summary": {
        "query": "user_activity_summary.sql",
        "description": "Top-10 найактивніших користувачів",
        "solves_task": "Завдання 5: Отримати 10 найкращих користувачів, які найчастіше клікали на рекламу.",
        "handler": "simple"
    },
    "campaigns_needing_budget": {
        "query": "campaign_summary.sql",
        "description": "Кампанії, що вичерпують бюджет",
        "solves_task": "Завдання 6: Визначити кампанії, які витратили понад 80% свого загального бюджету.",
        "handler": "budget_alert"
    },
    "device_summary": {
        "query": "device_summary.sql",
        "description": "Ефективність за типом пристроїв",
        "solves_task": "Завдання 7: Порівняти CTR на різних типах пристроїв (мобільний, десктоп, планшет).",
        "handler": "simple"
    }
}


def run_simple_report(report_name, config):
    """Обробник для простих звітів."""
    print(f"▶ Generating report: {config['description']}")
    df = fetch_data_from_query(config['query'])
    save_report(df, report_name)
    print(df.to_string())
    print(f"✓ Report '{report_name}' generated successfully.")


def run_top_5_ctr_report(report_name, config):
    """Обробник, що вибирає топ-5 по CTR."""
    print(f"▶ Generating report: {config['description']}")
    df = fetch_data_from_query(config['query'])
    # Сортуємо дані, отримані із загального звіту
    top_5_df = df.sort_values(by='CTR_Percentage', ascending=False).head(5)
    save_report(top_5_df, report_name)
    print(top_5_df.to_string())
    print(f"✓ Report '{report_name}' generated successfully.")


def run_budget_alert_report(report_name, config):
    """Обробник, що фільтрує кампанії з високим використанням бюджету."""
    print(f"▶ Generating report: {config['description']}")
    df = fetch_data_from_query(config['query'])
    # Фільтруємо дані
    alert_df = df[df['BudgetConsumptionPercentage'] > 80]
    save_report(alert_df, report_name)
    print(alert_df.to_string())
    print(f"✓ Report '{report_name}' generated successfully.")


# Мапа обробників
REPORT_HANDLERS = {
    "simple": run_simple_report,
    "top_5_ctr": run_top_5_ctr_report,
    "budget_alert": run_budget_alert_report,
}


def main():
    # Перетворюємо конфіг в нумерований список для вибору
    report_list = list(REPORTS_CONFIG.items())

    print("Welcome to the AdTech Data Analysis Tool!")
    print("Please choose a report to generate:")
    print("-" * 60)

    # Виводимо меню
    for i, (name, config) in enumerate(report_list):
        print(f"  {i + 1}. {config['description']}")
        print(f"     \033[42m{config['solves_task']}\033[0m")  # Виводимо опис сірим кольором
        print()  # Додаємо порожній рядок для кращої читабельності

    print("-" * 60)
    print("Enter 'all' to generate all reports.")

    while True:
        choice = input("Enter the number of the report (or 'all'): ").strip().lower()

        if choice == 'all':
            reports_to_run = report_list
            break

        try:
            choice_index = int(choice) - 1
            if 0 <= choice_index < len(report_list):
                reports_to_run = [report_list[choice_index]]
                break
            else:
                print("Invalid number. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number or 'all'.")

    # Запускаємо вибрані звіти
    for name, config in reports_to_run:
        handler_key = config.get("handler", "simple")
        handler_func = REPORT_HANDLERS.get(handler_key)

        if handler_func:
            try:
                handler_func(name, config)
            except Exception as e:
                print(f"✗ Failed to generate report '{name}'. Error: {e}")
        else:
            print(f"✗ Handler '{handler_key}' not found for report '{name}'.")

        print("-" * 60)


if __name__ == "__main__":
    main()
