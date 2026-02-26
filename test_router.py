import os
import json
import requests
from textwrap import dedent
from string import Template

"""
This script sends a single request to an OpenRouter LLM (with web search enabled)
to produce a structured, source-cited table for a given drug/product name.

IMPORTANT:
- Using :online / web search on OpenRouter can cost credits even if the model is free.
- Set env var OPENROUTER_API_KEY before running.
"""

OPENROUTER_API_KEY = "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"
if not OPENROUTER_API_KEY:
    raise RuntimeError("Set OPENROUTER_API_KEY env var")

# Recommended: keep this model stable for consistent formatting
MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-120b:online")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# ---- Prompt template ----
PROMPT_TEMPLATE = Template(dedent(r"""
РОЛЬ: Ты — фарма-аналитик. Ты извлекаешь данные только из ОТКРЫТЫХ источников и никогда не выдумываешь ссылки/PMID/NCT/РУ/значения.

ВХОД:
$drug_query

АНТИ-ГАЛЛЮЦИНАЦИОННЫЕ ПРАВИЛА:
1) Запрещено генерировать URL “по шаблону” (например vidal.ru/...__12345), PMID “наугад”, NCT “наугад”.
   Любая ссылка должна быть реально найдена через web search и подтверждена по контенту/сниппету.
   Если нет подтверждения — NOT FOUND.
2) Запрещено писать “пример”, “скорее всего”, “обычно” как значение.
3) Перед тем как вставить ссылку, проверь: в контенте/сниппете есть нужный препарат и (если задано) форма/доза.
4) Если поле не найдено — верни NOT FOUND и перечисли, где искал (реальные URL страниц поиска/карточек).

ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ДЛЯ МАКСИМАЛЬНОГО ЗАПОЛНЕНИЯ ПОЛЕЙ (PRAGMATIC MODE):
Цель: получить как можно больше фактических значений по препарату из открытых источников.
Если официальный/регуляторный источник не найден быстро, РАЗРЕШАЕТСЯ использовать открытые альтернативные источники
(аптечные карточки, агрегаторы инструкций, справочники, открытые PDF-инструкции), но только если:
- ссылка реальная,
- на странице явно совпадает препарат (торговое название/МНН),
- совпадает форма и дозировка (если указаны во входе).

ВАЖНО:
1) Не останавливайся после 1-2 неудачных попыток.
   Перед тем как поставить NOT FOUND по полю, проверь несколько типов источников:
   - VIDAL / GRLS витрины / инструкции PDF
   - открытые фарм-справочники / агрегаторы инструкций
   - аптечные карточки (если содержат структурированные данные)
   - PubMed / PMC / ClinicalTrials / ICHGCP (для PK и дизайна)
2) Если один источник не содержит поле, но содержит другой — заполняй поле из другого источника.
   (Например: состав из одной карточки, хранение из другой, PK из статьи.)
3) Если найдено несколько значений одного поля:
   - выбери наиболее конкретное значение,
   - в notes укажи, что были альтернативы.
4) Для PK-полей разрешается использовать:
   - статьи по МНН (даже если там не указан конкретный бренд),
   - если совпадают путь введения/форма/дозировка или близкий режим и это явно указано.
   В таком случае в locator/notes укажи контекст исследования (доза, healthy volunteers, fasted/fed).
5) Для поля "Тип высвобождения" разрешается inference:
   - если нет признаков modified/prolonged/retard/SR/ER/MR -> IR (status=inferred).
6) Для поля "Путь введения" разрешается inference:
   - если форма = таблетки/капсулы/оральный раствор и нет противоречий -> перорально (status=inferred).
7) Для поля "NTI/HVD":
   - если CVintra нет -> NTI=UNKNOWN; HVD=UNKNOWN (не выдумывать).
8) Для поля "kel":
   - если нет прямого значения, но есть T1/2 -> рассчитать kel = ln(2)/T1/2 и пометить status=calculated.

ПРАВИЛО ПРОТИВ ЛЕНИВОГО NOT FOUND:
Перед тем как вернуть NOT FOUND для поля, сделай минимум:
- 1 попытку в RU-карточках/инструкциях (для продуктовых полей)
- 1 попытку в альтернативных открытых карточках/агрегаторах (если RU-карточка не дала результата)
- 1 попытку в PubMed/PMC/ClinicalTrials (для PK/дизайна)
Если после этого нет — тогда NOT FOUND.

ПРАВИЛО РАЗДЕЛЕНИЯ ИСТОЧНИКОВ:
- Продуктовые поля (состав, хранение, производитель, форма, доза, РУ) ищи в карточках/инструкциях.
- PK и дизайн исследования (Cmax/AUC/Tmax/T1/2/CVintra/N/washout/fasted/fed) ищи в статьях/реестрах.
- Не пытайся брать PK из аптечной карточки, если там нет первичного источника.

ЦЕЛЕВЫЕ ПОЛЯ:
- INN/МНН
- Лекарственная форма
- Тип препарата (NTI/HVD)
- Путь введения
- Дозировка
- Состав: API + эксципиенты
- Условия хранения
- Производитель / держатель
- Условия приёма (fasted/fed, если это из исследования; в инструкции может не быть)
- Тип высвобождения (IR/SR/ER)
- РУ ЛП-№ (РФ)
- PK: T1/2, Tmax, Cmax, kel, AUC0-t, AUC0-inf, CVintra
- Метод выведения
- Дизайн исследования, N, washout

ИСТОЧНИКИ (приоритет, только открытые):
A) RU карточка/инструкция:
  1) VIDAL (RU) — vidal.ru (предпочитай полные карточки с подробным составом)
  2) GRLS витрины/зеркала без регистрации (например grls.pharm-portal.ru)
  3) Официальный сайт производителя (PDF инструкции), если доступно
  4) Открытые агрегаторы инструкций / аптечные карточки (fallback, если выше не дали поле)

B) PK/BE источники:
  5) PubMed (pubmed.ncbi.nlm.nih.gov) + PMC (pmc.ncbi.nlm.nih.gov) приоритетно
  6) ClinicalTrials.gov (дизайн/N/fasted/washout)
  7) ICHGCP mirror (ichgcp.net) как вспомогательный доступ

АЛГОРИТМ (универсальный):
ШАГ 1 — Нормализация
1.1. Из drug_query извлеки: торговое название/МНН/форма/доза/производитель/страна (что есть).
1.2. Найди RU карточку (VIDAL или GRLS витрина). Из карточки извлеки:
     - МНН (рус)
     - INN (англ) или латинское название (если указано)
     - ключевые синонимы/альтернативные названия (если есть)
     Если INN_EN/синонимы не указаны явно — попробуй получить их из первых результатов поиска по МНН.

ШАГ 2 — Продуктовые поля
2.1. Из RU карточки/инструкции вытащи: форма, дозировка, производитель/держатель, РУ, состав (API+excipients), хранение, метод выведения (если есть).
2.2. Если каких-то полей нет — добери их из альтернативных открытых карточек/агрегаторов/инструкций PDF.
2.3. Тип высвобождения:
     - SR/ER, если в названии/описании явно указано SR/ER/MR/retard/prolong/modif.
     - иначе IR (пометить inferred) и указать на чём основано.

ШАГ 3 — PK/BE поиск (строго от найденных синонимов!)
3.1. Сформируй PK-запросы, используя найденные INN_EN/синонимы:
     ("INN_EN" OR "synonym1" OR "synonym2") AND (bioequivalence OR crossover OR pharmacokinetics)
     AND (Cmax OR AUC OR Tmax OR half-life OR "intra-subject" OR CV)
     + добавь form/dose/route, если помогает.
3.2. Выбери 1–2 первичных источника с цифрами (таблица/результаты).
3.3. Извлеки PK и дизайн: fed/fasted, N, washout, design.
     Если цифр нет — NOT FOUND, но с реальной ссылкой и указанием “нет результатов”.

ШАГ 4 — NTI/HVD
4.1. NTI: если нет официального утверждения — UNKNOWN.
4.2. HVD: если найден CVintra:
     - CVintra >= 30% => HVD=YES
     - иначе HVD=NO
     Если CVintra нет => HVD=UNKNOWN.

ОБЯЗАТЕЛЬНЫЙ SELF-CHECK ПЕРЕД ВЫВОДОМ:
- Убедись, что все URL реальны (не шаблоны) и соответствуют источникам.
- Убедись, что нет выдуманных PMID/NCT/РУ.
- Если сомневаешься — NOT FOUND.

ВЫВОД:
1) Сначала Markdown-таблица:
| Поле | Значение | Источник (URL) | Где именно |

2) Затем JSON (строго валидный), структура:
{
  "drug_query": "...",
  "chosen_product": {
    "trade_name": "...",
    "inn": "...",
    "dosage_form": "...",
    "strength": "...",
    "manufacturer_or_holder": "...",
    "ru_reg_number": "..."
  },
  "attributes": {
     "<field>": {"value": "...|null", "status": "ok|missing|unknown|inferred|calculated", "source_url": "...", "locator": "..."}
  },
  "notes": ["что удалось/не удалось найти и где искали"],
  "search_notes": [
    "кратко: какие источники дали поля, какие не дали",
    "если PK взяты по МНН без бренда — указать контекст"
  ]
}
""").strip())

def build_messages(drug_query: str):
    user_prompt = PROMPT_TEMPLATE.substitute(drug_query=drug_query)
    return [
        {
            "role": "system",
            "content": "Ты аккуратный аналитик. Не выдумывай факты. Всегда давай источники и где именно найдено."
        },
        {"role": "user", "content": user_prompt},
    ]

def openrouter_chat(drug_query: str):
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        # Optional but recommended by OpenRouter:
        "HTTP-Referer": os.getenv("OPENROUTER_REFERER", "https://local-script"),
        "X-Title": os.getenv("OPENROUTER_TITLE", "Pharma Reference Extractor"),
    }
    payload = {
        "model": MODEL,
        "messages": build_messages(drug_query),
        "temperature": 0.2,
        "max_tokens": 7000,
    }
    r = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=180)
    # If you get 402 here, it often means web search credits are required for :online
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]


# Example input mirroring your case:
drug_query = 'Ксеникал (капсулы,  120 мг; Чеплафарм Арцнаймиттель ГмбХ, Германия)'
result = openrouter_chat(drug_query)
print(result)
