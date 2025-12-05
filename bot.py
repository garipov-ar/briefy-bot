import logging
import os
from dotenv import load_dotenv
from io import BytesIO
import pandas as pd
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан в .env")

async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc.file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("Пожалуйста, отправьте файл в формате .xlsx")
        return

    file_name = doc.file_name
    file_obj = await doc.get_file()
    file_bytes = BytesIO()
    await file_obj.download_to_memory(file_bytes)
    file_bytes.seek(0)

    try:
        # Читаем Excel, начиная с 3-й строки как заголовков (header=2)
        df = pd.read_excel(file_bytes, header=2)
    except Exception as e:
        logger.error(f"Ошибка чтения Excel: {e}")
        await update.message.reply_text("❌ Не удалось прочитать Excel-файл.")
        return

    # Проверяем обязательные столбцы
    required_cols = [
        '"source_NTTM_DB"[3ЛТП_Признак]',
        'Уровень',
        'Исключить ЦЭ',
        'Исключить по услуге',
        'Тип услуги',
        'Нарушение SLA',
        'Нарушение SLA без ожидания клиента'
    ]
    if not all(col in df.columns for col in required_cols):
        await update.message.reply_text("❌ В файле отсутствуют необходимые столбцы.")
        return

    # === Обработка по типу файла (DWH / SLA) ===
    if "dwh" in file_name.lower():
        logger.info(f"Обрабатываю DWH-файл: {file_name}")
        mask_ott = df["Тип услуги"] == "ОТТ"
        df.loc[mask_ott, "Нарушение SLA"] = df.loc[mask_ott, "Нарушение SLA без ожидания клиента"].apply(
            lambda x: 1 if x == 1 else 0
        )
    elif "sla" in file_name.lower():
        logger.info(f"Обрабатываю SLA-файл: {file_name}")
        mask_ott = df["Тип услуги"] == "ОТТ"
        df.loc[mask_ott, "Нарушение SLA"] = df.loc[mask_ott, "Нарушение SLA без ожидания клиента"].apply(
            lambda x: 1 if x == 1 else 0
        )
    else:
        await update.message.reply_text("ℹ️ Имя файла должно содержать 'dwh' или 'sla'.")
        return

    # === Фильтрация общих условий ===
    base_mask = (
        (df['"source_NTTM_DB"[3ЛТП_Признак]'] == 1) &
        (df['Исключить ЦЭ'] == 'Без признака ЦЭ') &
        (df['Исключить по услуге'] == 'Расчетные услуги')
    )

    # === Отчёт 1: Платиновый ===
    platina_mask = base_mask & (df['Уровень'] == 'Платиновый')
    df_platina = df[platina_mask].copy()
    total_platina = len(df_platina)

    if total_platina > 0:
        # Приведение "Нарушение SLA" к числу ("" → NaN → 1, т.к. не в срок)
        df_platina['Нарушение SLA'] = pd.to_numeric(df_platina['Нарушение SLA'], errors='coerce').fillna(1)
        on_time_platina = (df_platina['Нарушение SLA'] == 0).sum()
        sla_platina = round(on_time_platina / total_platina * 100, 1)
    else:
        sla_platina = "—"

    # === Отчёт 2: Прочие уровни (Бронзовый, Золотой, Серебряный) ===
    other_levels = ['Бронзовый', 'Золотой', 'Серебряный']
    other_mask = base_mask & (df['Уровень'].isin(other_levels))
    df_other = df[other_mask].copy()
    total_other = len(df_other)

    if total_other > 0:
        df_other['Нарушение SLA'] = pd.to_numeric(df_other['Нарушение SLA'], errors='coerce').fillna(1)
        on_time_other = (df_other['Нарушение SLA'] == 0).sum()
        sla_other = round(on_time_other / total_other * 100, 1)
    else:
        sla_other = "—"

    # === Формирование ответа ===
    def calc_sla_report(total, on_time):
        if total == 0:
            return "—", "—", "—"
        
        sla_pct = round(on_time / total * 100, 1)
        min_on_time = int(total * 0.87)  # можно использовать math.ceil, но int(x*0.87) + (1 if x*0.87%1 else 0) — проще через ceil
        import math
        min_on_time = math.ceil(total * 0.87)
        buffer = on_time - min_on_time  # сколько "лишних" ТТ в срок — можно позволить столько же нарушений

        if buffer >= 0:
            status = f"✅ В норме (+{buffer} ТТ)"
        else:
            status = f"❌ Ниже норматива ({buffer} ТТ не хватает)"

        return sla_pct, buffer, status

    # Применяем к Платине
    if total_platina > 0:
        df_platina['Нарушение SLA'] = pd.to_numeric(df_platina['Нарушение SLA'], errors='coerce').fillna(1)
        on_time_platina = (df_platina['Нарушение SLA'] == 0).sum()
        sla_platina, buffer_platina, status_platina = calc_sla_report(total_platina, on_time_platina)
    else:
        sla_platina = buffer_platina = status_platina = "—"
        on_time_platina = 0

    # Применяем к Прочим
    if total_other > 0:
        df_other['Нарушение SLA'] = pd.to_numeric(df_other['Нарушение SLA'], errors='coerce').fillna(1)
        on_time_other = (df_other['Нарушение SLA'] == 0).sum()
        sla_other, buffer_other, status_other = calc_sla_report(total_other, on_time_other)
    else:
        sla_other = buffer_other = status_other = "—"
        on_time_other = 0

    # Формируем отчёт
    report = (
        "📊 Отчёт по SLA (3ЛТП), норматив: **87,0%**\n\n"
        f"🔹 **Платиновый**\n"
        f"   Всего: {total_platina}\n"
        f"   В срок: {on_time_platina}\n"
        f"   SLA: {sla_platina}%\n"
        f"   Статус: {status_platina}\n\n"
        f"🔹 **Прочие уровни** (Бронза/Золото/Серебро)\n"
        f"   Всего: {total_other}\n"
        f"   В срок: {on_time_other}\n"
        f"   SLA: {sla_other}%\n"
        f"   Статус: {status_other}"
    )

    await update.message.reply_text(report, parse_mode="Markdown")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"), handle_excel))
    logger.info("Бот запущен...")
    app.run_polling()

if __name__ == '__main__':
    main()