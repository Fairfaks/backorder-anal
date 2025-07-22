import pandas as pd
import asyncio
import aiohttp
import time
from datetime import datetime

INPUT_FILE = 'объединенная_таблица.xlsx'
OUTPUT_FILE = 'результат_backorder.xlsx'
DOMAIN_COLUMN = 'Domain'
MAX_CONCURRENT = 60

async def fetch_backorder_data(session, domain, sem):
    url = (
        f"https://backorder.ru/json/"
        f"?order=desc&domainname={domain}"
        f"&view_all=1&by=hotness&page=1&items=1"
    )
    async with sem:
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        entry = data[0]
                        entry['domain'] = domain
                        return entry
        except Exception as e:
            return {'domain': domain, 'result': f'Ошибка: {e}'}
        return {'domain': domain, 'result': 'отсутствует на backorder'}

async def main():
    df = pd.read_excel(INPUT_FILE)
    domains = df[DOMAIN_COLUMN].dropna().astype(str).tolist()
    results = []
    total = len(domains)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_backorder_data(session, domain, sem) for domain in domains]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            result = await coro
            results.append(result)
            if i % 100 == 0 or i == total:
                percent = i / total * 100
                print(f'Обработано {i} из {total} доменов ({percent:.1f}%)')
            # time.sleep(0.05)  # можно убрать задержку — семафор уже ограничивает нагрузку

    # Собираем уникальные поля
    all_keys = set()
    for row in results:
        all_keys.update(row.keys())

    columns = ['domain'] + sorted([k for k in all_keys if k != 'domain'])
    rows = []
    for row in results:
        rows.append({k: row.get(k, '') for k in columns})

    df_all = pd.DataFrame(rows, columns=columns)

    # Столбец можно купить — только если домен найден и delete_date <= сегодня
    today = pd.Timestamp(datetime.now().date())
    can_buy = []
    for idx, row in df_all.iterrows():
        delete_date_raw = row.get('delete_date', '')
        if row.get('result', '') == 'отсутствует на backorder' or not delete_date_raw:
            can_buy.append('')
        else:
            try:
                delete_date = pd.to_datetime(delete_date_raw, errors='coerce')
                if pd.notnull(delete_date) and delete_date <= today:
                    can_buy.append('да')
                else:
                    can_buy.append('нет')
            except:
                can_buy.append('')
    df_all['можно купить'] = can_buy

    # Лист с отсутствующими
    df_missing = df_all[df_all['result'] == 'отсутствует на backorder'].copy()

    # Сохраняем оба листа
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        df_all.to_excel(writer, index=False, sheet_name='Все домены')
        df_missing.to_excel(writer, index=False, sheet_name='Нет на backorder')

    print(f'Готово! Сохранено в {OUTPUT_FILE}')

if __name__ == "__main__":
    asyncio.run(main())
