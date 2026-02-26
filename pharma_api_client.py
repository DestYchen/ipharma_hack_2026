from __future__ import annotations

import json

import requests


def prompt_non_empty(label: str) -> str:
    while True:
        value = input(f"{label}: ").strip()
        if value:
            return value
        print("Поле не должно быть пустым.")


def choose_option(max_index: int) -> int:
    while True:
        raw = input(f"Выберите номер референта (1-{max_index}): ").strip()
        if raw.isdigit():
            value = int(raw)
            if 1 <= value <= max_index:
                return value
        print("Некорректный номер.")


def main() -> None:
    base_url = input("URL локального API (Enter = http://127.0.0.1:8000): ").strip() or "http://127.0.0.1:8000"
    xls_path = input("Путь к .xls (Enter = авто из текущей папки сервера): ").strip()

    print("\nВведите параметры поиска:")
    payload = {
        "mnn": prompt_non_empty("МНН"),
        "routes": prompt_non_empty("Пути введения"),
        "base_form": prompt_non_empty("Базовая лекарственная форма"),
        "release_type": prompt_non_empty("Тип высвобождения"),
        "dosage": prompt_non_empty("Дозировка"),
    }
    if xls_path:
        payload["xls_path"] = xls_path

    resp = requests.post(f"{base_url}/reference/search", json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Ошибка API: {data}")

    if data.get("matches_count", 0) == 0:
        print("\nСовпадения не найдены.")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    print(f"\nСовпадений строк: {data['matches_count']}")
    print(f"Вариантов референта: {data['reference_options_count']}")
    for i, option in enumerate(data["reference_options"], start=1):
        print(f"{i}. {option['reference_drug']} (строк: {option['rows_count']})")
        for sample in option.get("sample_rows", []):
            print(f"   - {sample['trade_name']} | {sample['drug_form']} | {sample['dosage']}")

    option_index = choose_option(data["reference_options_count"])

    save_json_path = input("Куда сохранить JSON выбора (Enter = авто): ").strip()
    run_router = (input("Запустить test_router.py по выбранному референту? (Y/n): ").strip().lower() or "y") == "y"

    if run_router:
        save_router_output_path = input("Куда сохранить ответ test_router (Enter = авто): ").strip()
        analyze_payload = {
            "session_id": data["session_id"],
            "option_index": option_index,
        }
        if save_json_path:
            analyze_payload["save_json_path"] = save_json_path
        if save_router_output_path:
            analyze_payload["save_router_output_path"] = save_router_output_path

        resp2 = requests.post(f"{base_url}/pipeline/analyze", json=analyze_payload, timeout=600)
        resp2.raise_for_status()
        result = resp2.json()
        if not result.get("ok"):
            raise RuntimeError(f"Ошибка API: {result}")

        selection = result["selection"]
        router = result["router"]
        print("\nГотово.")
        print(f"Выбран референт: {selection['selected_reference_drug']}")
        print(f"JSON выбора: {selection['saved_json_path']}")
        print(f"Ответ test_router сохранен: {router['saved_response_path']}")
    else:
        choose_payload = {
            "session_id": data["session_id"],
            "option_index": option_index,
        }
        if save_json_path:
            choose_payload["save_json_path"] = save_json_path

        resp2 = requests.post(f"{base_url}/reference/choose", json=choose_payload, timeout=120)
        resp2.raise_for_status()
        result = resp2.json()
        if not result.get("ok"):
            raise RuntimeError(f"Ошибка API: {result}")

        print("\nГотово.")
        print(f"Выбран референт: {result['selected_reference_drug']}")
        print(f"JSON выбора: {result['saved_json_path']}")


if __name__ == "__main__":
    main()
