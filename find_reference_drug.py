from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


SHEET_NAME = "ст27.1 ФЗ-61"
SHEET_INDEX_FALLBACK = 3


COL_REFERENCE = "Референтный препарат"
COL_MNN = "МНН (группировочное или химическое наименование)"
COL_TRADE = "Торговое наименование"
COL_FORM = "Лекарственная форма"
COL_DOSAGE = "Дозировка"
COL_OWNER = "Владелец РУ"
COL_COUNTRY = "Страна"
COL_RU_NUMBER = "Номер РУ"
COL_RU_DATE = "Дата  РУ"
COL_EXCEPTIONS = "Исключение отдельных групп пациентов"


BASE_FORM_KEYWORDS = [
    "имплантат",
    "таблетки",
    "капсулы",
    "лиофилизат",
    "порошок",
    "гранулы",
    "концентрат",
    "растворитель",
    "раствор",
    "суспензия",
    "эмульсия",
    "сироп",
    "капли",
    "спрей",
    "аэрозоль",
    "пластырь",
    "суппозитории",
    "мазь",
    "крем",
    "гель",
    "лосьон",
    "пена",
    "шампунь",
    "паста",
    "линимент",
    "настойка",
    "экстракт",
]


@dataclass(frozen=True)
class ParsedForm:
    raw: str
    base_form: str
    release_type: str
    routes: list[str]


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value)
    text = text.replace("\xa0", " ")
    text = text.replace("ё", "е").replace("Ё", "Е")
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_compact(value: Any) -> str:
    text = normalize_text(value)
    return re.sub(r"[\s,;]+", "", text)


def first_xls_in_dir(directory: Path) -> Path:
    files = sorted(directory.glob("*.xls"))
    if not files:
        raise FileNotFoundError("В текущей папке не найдено ни одного .xls файла")
    return files[0]


def load_sheet(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_excel(path, sheet_name=SHEET_NAME, engine="xlrd")
    except Exception:
        df = pd.read_excel(path, sheet_name=SHEET_INDEX_FALLBACK, engine="xlrd")

    required = [
        COL_REFERENCE,
        COL_MNN,
        COL_TRADE,
        COL_FORM,
        COL_DOSAGE,
        COL_OWNER,
        COL_COUNTRY,
        COL_RU_NUMBER,
        COL_RU_DATE,
        COL_EXCEPTIONS,
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"В листе отсутствуют ожидаемые колонки: {missing}")

    df = df.copy()
    # В xls есть объединенные ячейки: pandas читает только верхнюю ячейку, остальные становятся NaN.
    for column in [COL_REFERENCE, COL_MNN, COL_FORM, COL_EXCEPTIONS]:
        df[column] = df[column].ffill()

    df = df[df[COL_TRADE].notna()].copy()
    df = df[df[COL_REFERENCE].notna()].copy()

    df["__parsed_form__"] = df[COL_FORM].fillna("").map(parse_form)
    df["parsed_base_form"] = df["__parsed_form__"].map(lambda p: p.base_form)
    df["parsed_release_type"] = df["__parsed_form__"].map(lambda p: p.release_type)
    df["parsed_routes"] = df["__parsed_form__"].map(lambda p: p.routes)
    df["mnn_norm"] = df[COL_MNN].map(normalize_text)
    df["dosage_norm"] = df[COL_DOSAGE].map(normalize_text)
    df["dosage_compact"] = df[COL_DOSAGE].map(normalize_compact)
    return df


def parse_form(form_value: Any) -> ParsedForm:
    raw = "" if pd.isna(form_value) else str(form_value)
    text = normalize_text(raw)
    base_form = extract_base_form(text)
    release_type = extract_release_type(text)
    routes = extract_routes(text, base_form)
    return ParsedForm(raw=raw, base_form=base_form, release_type=release_type, routes=routes)


def extract_base_form(form_text: str) -> str:
    if not form_text:
        return ""
    for keyword in BASE_FORM_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", form_text):
            return keyword
    first_word = form_text.split(",", 1)[0].split(" ", 1)[0]
    return first_word


def extract_release_type(form_text: str) -> str:
    if not form_text:
        return ""
    # Порядок важен: более специфичные типы выше.
    release_rules = [
        ("кишечнорастворимое", [r"кишечнораствор"]),
        (
            "пролонгированное",
            [
                r"пролонгированн\w* высвобожд",
                r"пролонгированного действия",
                r"ретард",
            ],
        ),
        (
            "модифицированное",
            [
                r"модифицированн\w* высвобожд",
                r"замедленн\w* высвобожд",
                r"контролируем\w* высвобожд",
                r"длительного высвобожд",
            ],
        ),
    ]
    for canonical, patterns in release_rules:
        if any(re.search(pattern, form_text) for pattern in patterns):
            return canonical
    return "обычное"


def extract_routes(form_text: str, base_form: str) -> list[str]:
    routes: set[str] = set()
    text = form_text

    route_patterns = [
        ("внутривенно", [r"внутривенн\w*", r"\bв/в\b"]),
        ("внутримышечно", [r"внутримышечн\w*", r"\bв/м\b"]),
        ("подкожно", [r"подкожн\w*", r"\bп/к\b"]),
        ("внутрикожно", [r"внутрикожн\w*"]),
        ("ингаляционно", [r"для ингаляц", r"\bингаляц"]),
        ("назально", [r"назальн", r"интраназальн"]),
        ("глазно", [r"глазн"]),
        ("ушно", [r"ушн"]),
        ("ректально", [r"ректальн"]),
        ("вагинально", [r"вагинальн"]),
        ("наружно", [r"наружн\w* применен", r"накожн\w*"]),
        ("местно", [r"местн\w* применен"]),
        ("перорально", [r"для приема внутрь", r"перорал"]),
        ("трансдермально", [r"трансдермальн"]),
        ("внутриполостно", [r"внутриполостн"]),
        ("внутрисосудисто", [r"внутрисосудист"]),
        ("внутрипузырно", [r"внутрипузыр"]),
        ("инъекционно", [r"для инъекц"]),
        ("для инфузий", [r"для инфуз"]),
    ]

    for canonical, patterns in route_patterns:
        if any(re.search(pattern, text) for pattern in patterns):
            routes.add(canonical)

    if not routes:
        oral_forms = {
            "таблетки",
            "капсулы",
            "гранулы",
            "суспензия",
            "сироп",
            "порошок",
            "капли",
            "паста",
            "настойка",
            "экстракт",
        }
        topical_forms = {"крем", "мазь", "гель", "лосьон", "пена", "шампунь", "линимент"}
        if base_form in oral_forms:
            routes.add("перорально")
        elif base_form in topical_forms:
            routes.add("наружно")
        elif base_form == "суппозитории":
            # Без уточнения в форме однозначно определить нельзя.
            pass
        elif base_form == "пластырь":
            routes.add("трансдермально")

    return sorted(routes)


def parse_user_routes(route_value: str) -> set[str]:
    text = normalize_text(route_value)
    if not text:
        return set()
    parsed = set(extract_routes(text, ""))
    if parsed:
        return parsed

    # fallback: разбить по типовым разделителям и сравнивать строкой
    chunks = re.split(r"[,;/]| и ", text)
    return {normalize_text(chunk) for chunk in chunks if normalize_text(chunk)}


def normalize_release_type_user(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    if "кишечно" in text:
        return "кишечнорастворимое"
    if "пролонг" in text or "ретард" in text:
        return "пролонгированное"
    if "модифиц" in text or "контролируем" in text or "замед" in text:
        return "модифицированное"
    if text in {"обычное", "немодифицированное", "без модификации"}:
        return "обычное"
    return text


def normalize_base_form_user(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    stem_aliases = [
        ("таблетки", ["таблетк"]),
        ("капсулы", ["капсул"]),
        ("раствор", ["раствор"]),
        ("порошок", ["порош"]),
        ("лиофилизат", ["лиофилиз"]),
        ("гранулы", ["гранул"]),
        ("суспензия", ["суспенз"]),
        ("аэрозоль", ["аэрозол"]),
        ("спрей", ["спре"]),
        ("капли", ["капл"]),
        ("суппозитории", ["суппозитор"]),
        ("гель", ["гел"]),
        ("крем", ["крем"]),
        ("мазь", ["маз"]),
        ("пластырь", ["пластыр"]),
        ("концентрат", ["концентрат"]),
    ]
    for canonical, stems in stem_aliases:
        if any(stem in text for stem in stems):
            return canonical
    for keyword in BASE_FORM_KEYWORDS:
        if keyword in text:
            return keyword
    return text


def dosage_matches(user_dosage: str, candidate_dosage: Any) -> bool:
    user_norm = normalize_text(user_dosage)
    if not user_norm:
        return True
    cand_norm = normalize_text(candidate_dosage)
    if not cand_norm:
        return False
    if user_norm == cand_norm:
        return True

    # Частый кейс: пользователь вводит одну дозировку, а в таблице список через запятую.
    parts = [normalize_text(part) for part in re.split(r"[;,]", cand_norm) if normalize_text(part)]
    if user_norm in parts:
        return True

    # Еще один fallback: сравнение без пробелов/разделителей.
    user_compact = normalize_compact(user_dosage)
    if user_compact == normalize_compact(candidate_dosage):
        return True
    part_compacts = [normalize_compact(part) for part in parts]
    return user_compact in part_compacts


def row_matches(
    row: pd.Series,
    mnn: str,
    routes: set[str],
    base_form: str,
    release_type: str,
    dosage: str,
) -> bool:
    if normalize_text(mnn) != row["mnn_norm"]:
        return False

    if base_form and normalize_base_form_user(base_form) != normalize_text(row["parsed_base_form"]):
        return False

    if release_type:
        user_release = normalize_release_type_user(release_type)
        if user_release != normalize_text(row["parsed_release_type"]):
            return False

    if routes:
        row_routes = set(row["parsed_routes"] or [])
        if not row_routes:
            return False
        if not routes.issubset(row_routes):
            return False

    if dosage and not dosage_matches(dosage, row[COL_DOSAGE]):
        return False

    return True


def format_row_brief(row: pd.Series) -> str:
    routes = ", ".join(row["parsed_routes"]) if row["parsed_routes"] else "не определен"
    return (
        f"Референтный: {row[COL_REFERENCE]} | ТН: {row[COL_TRADE]} | "
        f"Форма: {row[COL_FORM]} | Дозировка: {row[COL_DOSAGE]} | "
        f"Базовая форма: {row['parsed_base_form']} | Тип высвобождения: {row['parsed_release_type']} | "
        f"Путь: {routes}"
    )


def build_output_payload(
    query: dict[str, str],
    chosen_reference: str,
    chosen_rows: pd.DataFrame,
    all_reference_options: list[dict[str, Any]],
    source_file: Path,
) -> dict[str, Any]:
    rows_payload: list[dict[str, Any]] = []
    for _, row in chosen_rows.iterrows():
        ru_date = row[COL_RU_DATE]
        if pd.notna(ru_date):
            if isinstance(ru_date, pd.Timestamp):
                ru_date_value = ru_date.strftime("%Y-%m-%d")
            elif isinstance(ru_date, (datetime, date)):
                ru_date_value = ru_date.strftime("%Y-%m-%d")
            else:
                ru_date_value = str(ru_date)
        else:
            ru_date_value = None

        rows_payload.append(
            {
                "reference_drug": str(row[COL_REFERENCE]),
                "mnn": str(row[COL_MNN]),
                "trade_name": str(row[COL_TRADE]),
                "drug_form": None if pd.isna(row[COL_FORM]) else str(row[COL_FORM]),
                "dosage": None if pd.isna(row[COL_DOSAGE]) else str(row[COL_DOSAGE]),
                "owner_ru": None if pd.isna(row[COL_OWNER]) else str(row[COL_OWNER]),
                "country": None if pd.isna(row[COL_COUNTRY]) else str(row[COL_COUNTRY]),
                "ru_number": None if pd.isna(row[COL_RU_NUMBER]) else str(row[COL_RU_NUMBER]),
                "ru_date": ru_date_value,
                "patient_exceptions": None if pd.isna(row[COL_EXCEPTIONS]) else str(row[COL_EXCEPTIONS]),
                "parsed": {
                    "base_form": row["parsed_base_form"],
                    "release_type": row["parsed_release_type"],
                    "routes": list(row["parsed_routes"] or []),
                },
            }
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": {
            "file": str(source_file),
            "sheet": SHEET_NAME,
            "sheet_fallback_index": SHEET_INDEX_FALLBACK,
        },
        "query": query,
        "selected_reference_drug": chosen_reference,
        "selected_reference_rows_count": len(rows_payload),
        "selected_reference_rows": rows_payload,
        "reference_options_count": len(all_reference_options),
        "reference_options": all_reference_options,
    }


def choose_reference(reference_options: list[dict[str, Any]]) -> str:
    if not reference_options:
        raise ValueError("Нет вариантов для выбора")
    if len(reference_options) == 1:
        option = reference_options[0]
        print("\nНайден один референтный препарат:")
        print(f"1. {option['reference_drug']} (совпадений строк: {option['rows_count']})")
        return option["reference_drug"]

    print("\nНайдено несколько возможных референтных препаратов:")
    for idx, option in enumerate(reference_options, start=1):
        print(f"{idx}. {option['reference_drug']} (совпадений строк: {option['rows_count']})")
        sample_rows = option["sample_rows"]
        for sample in sample_rows:
            print(f"   - ТН: {sample['trade_name']} | Форма: {sample['drug_form']} | Дозировка: {sample['dosage']}")

    while True:
        answer = input("\nВведите номер референтного препарата: ").strip()
        if not answer.isdigit():
            print("Нужно ввести номер из списка.")
            continue
        choice = int(answer)
        if 1 <= choice <= len(reference_options):
            return reference_options[choice - 1]["reference_drug"]
        print("Номер вне диапазона.")


def prompt_non_empty(prompt: str) -> str:
    while True:
        value = input(prompt).strip()
        if value:
            return value
        print("Поле не должно быть пустым.")


def prompt_output_path(default_name: str) -> Path:
    raw = input(f"Путь для JSON (Enter = {default_name}): ").strip()
    return Path(raw) if raw else Path(default_name)


def main() -> None:
    workdir = Path.cwd()
    default_xls = first_xls_in_dir(workdir)
    xls_input = input(f"Путь к .xls (Enter = {default_xls.name}): ").strip()
    xls_path = Path(xls_input) if xls_input else default_xls
    if not xls_path.exists():
        raise FileNotFoundError(f"Файл не найден: {xls_path}")

    print("\nЗагрузка и анализ листа, подождите...")
    df = load_sheet(xls_path)

    print("\nВведите параметры поиска:")
    mnn = prompt_non_empty("МНН: ")
    routes_input = prompt_non_empty("Пути введения: ")
    base_form = prompt_non_empty("Базовая лекарственная форма: ")
    release_type = prompt_non_empty("Тип высвобождения: ")
    dosage = prompt_non_empty("Дозировка: ")

    user_routes = parse_user_routes(routes_input)
    if not user_routes:
        user_routes = {normalize_text(routes_input)}

    mask = df.apply(
        row_matches,
        axis=1,
        args=(mnn, user_routes, base_form, release_type, dosage),
    )
    matches = df[mask].copy()

    if matches.empty:
        print("\nСовпадения не найдены.")
        print("Подсказка: проверьте дозировку и тип высвобождения (например, 'обычное' / 'пролонгированное').")
        return

    print(f"\nНайдено совпадающих строк: {len(matches)}")
    for _, row in matches.head(10).iterrows():
        print(f"- {format_row_brief(row)}")
    if len(matches) > 10:
        print(f"... и еще {len(matches) - 10} строк")

    reference_options: list[dict[str, Any]] = []
    for ref_name, group in matches.groupby(COL_REFERENCE, sort=True):
        sample_rows = []
        for _, row in group.head(3).iterrows():
            sample_rows.append(
                {
                    "trade_name": None if pd.isna(row[COL_TRADE]) else str(row[COL_TRADE]),
                    "drug_form": None if pd.isna(row[COL_FORM]) else str(row[COL_FORM]),
                    "dosage": None if pd.isna(row[COL_DOSAGE]) else str(row[COL_DOSAGE]),
                }
            )
        reference_options.append(
            {
                "reference_drug": str(ref_name),
                "rows_count": int(len(group)),
                "sample_rows": sample_rows,
            }
        )

    reference_options.sort(key=lambda item: item["reference_drug"])
    chosen_reference = choose_reference(reference_options)
    chosen_rows = matches[matches[COL_REFERENCE] == chosen_reference].copy()

    query = {
        "mnn": mnn,
        "routes": routes_input,
        "base_form": base_form,
        "release_type": release_type,
        "dosage": dosage,
    }
    payload = build_output_payload(query, chosen_reference, chosen_rows, reference_options, xls_path)

    default_json = f"reference_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = prompt_output_path(default_json)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nJSON сохранен: {output_path}")


if __name__ == "__main__":
    main()
