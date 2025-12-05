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


# =====================================================================
# Универсальные функции
# =====================================================================

def normalize_sla_column(df):
    """
    Конвертирует 'Нарушение SLA' в 0/1.
    Пустые → 1 (нарушение).
    """
    return pd.to_numeric(df['Нарушение SLA'], errors='coerce').fillna(1)


def fix_ott(df):
    """
    Для ОТТ подменяет "Нарушение SLA" значением из "Нарушение SLA без ожидания клиента".
    """
    mask_ott = df["Тип услуги"] == "ОТТ"
    df.loc[mask_ott, "Нарушение SLA"] = df.loc[mask_ott, "Нарушение SLA без ожидания клиента"] \
        .apply(lambda x: 1 if x == 1 else 0)
    return df


def calc_sla(total, on_time):
    """Расчёт SLA и буфера до норматива."""
    if total == 0:
        return "—", "—", "—"

    import math
    sla_pct = round(on_time / total * 100, 1)
    min_on_time = math.ceil(total * 0.87)
    buffer = on_time - min_on_time

    status = f"✅(+{buffer} ТТ)" if buffer >= 0 else f"❌ Ниже норматива ({buffer} ТТ)"

    return sla_pct, buffer, status


# =====================================================================
# Обработчик Excel
# =====================================================================

async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document

    # Универсальная проверка Excel
    if not doc.file_name.lower().endswith(".xlsx"):
        await update.message.reply_text("Пожалуйста, отправьте файл в формате .xlsx")
        return

    file_name = doc.file_name.lower()

    # Загружаем файл
    file_bytes = BytesIO()
    await (await doc.get_file()).download_to_memory(file_bytes)
    file_bytes.seek(0)

    try:
        df = pd.read_excel(file_bytes, header=2)
    except Exception as e:
        logger.error(f"Ошибка чтения Excel: {e}")
        await update.message.reply_text("❌ Не удалось прочитать Excel-файл.")
        return

    # Проверка обязательных столбцов
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

    # Определяем тип файла
    if "dwh" in file_name or "sla" in file_name:
        df = fix_ott(df)
    else:
        await update.message.reply_text("ℹ️ Имя файла должно содержать 'dwh' или 'sla'.")
        return

    # Общая фильтрация
    base_mask = (
        (df['"source_NTTM_DB"[3ЛТП_Признак]'] == 1) &
        (df['Исключить ЦЭ'] == 'Без признака ЦЭ') &
        (df['Исключить по услуге'] == 'Расчетные услуги')
    )
    df = df[base_mask].copy()

    # ------------------- ПЛАТИНОВЫЙ -------------------
    df_platina = df[df['Уровень'] == 'Платиновый'].copy()
    df_platina['Нарушение SLA'] = normalize_sla_column(df_platina)

    total_platina = len(df_platina)
    on_time_platina = (df_platina['Нарушение SLA'] == 0).sum()
    sla_platina, buffer_platina, status_platina = calc_sla(total_platina, on_time_platina)

    # ------------------- ПРОЧИЕ (Бронза/Золото/Серебро) -------------------
    other_levels = ['Бронзовый', 'Золотой', 'Серебряный']
    df_other = df[df['Уровень'].isin(other_levels)].copy()
    df_other['Нарушение SLA'] = normalize_sla_column(df_other)

    total_other = len(df_other)
    on_time_other = (df_other['Нарушение SLA'] == 0).sum()
    sla_other, buffer_other, status_other = calc_sla(total_other, on_time_other)

    # =====================================================================
    # Формирование отчёта
    # =====================================================================

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


# =====================================================================
# main()
# =====================================================================

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.Document.ALL, handle_excel))

    logger.info("Бот запущен...")
    app.run_polling()


if __name__ == '__main__':
    main()
