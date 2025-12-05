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
# Вспомогательные функции
# =====================================================================

def normalize_sla_column(df):
    """Конвертирует 'Нарушение SLA' в 0/1. Пустые → 1 (нарушение)."""
    return pd.to_numeric(df['Нарушение SLA'], errors='coerce').fillna(1)


def fix_ott(df):
    """Для ОТТ подменяет 'Нарушение SLA' значением из 'Нарушение SLA без ожидания клиента'."""
    mask_ott = df["Тип услуги"] == "ОТТ"
    df.loc[mask_ott, "Нарушение SLA"] = df.loc[mask_ott, "Нарушение SLA без ожидания клиента"] \
        .apply(lambda x: 1 if x == 1 else 0)
    return df


def calc_sla(total, on_time, norm=0.87):
    """
    Расчёт SLA и количества новых ТТ, необходимых для достижения норматива.
    Новые ТТ учитываются и в total, и в on_time.
    """
    import math

    # Если всего нет, считаем SLA 100%
    if total == 0:
        return 100.0, 0, "✅"

    sla_pct = round(on_time / total * 100, 1)

    # Расчёт новых ТТ, которые нужно добавить
    diff = norm * total - on_time
    if diff <= 0:
        need_tt = 0
        status = "✅"
    else:
        need_tt = math.ceil(diff / (1 - norm))
        status = "❌"

    return sla_pct, need_tt, status


# =====================================================================
# Обработчик Excel
# =====================================================================

async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document

    if not doc.file_name.lower().endswith(".xlsx"):
        await update.message.reply_text("Пожалуйста, отправьте файл в формате .xlsx")
        return

    file_bytes = BytesIO()
    await (await doc.get_file()).download_to_memory(file_bytes)
    file_bytes.seek(0)

    try:
        df = pd.read_excel(file_bytes, header=2)
    except Exception as e:
        logger.error(f"Ошибка чтения Excel: {e}")
        await update.message.reply_text("❌ Не удалось прочитать Excel-файл.")
        return

    required_cols = [
        '"source_NTTM_DB"[3ЛТП_Признак]',
        'Уровень',
        'Исключить ЦЭ',
        'Исключить по услуге',
        'Тип услуги',
        'Нарушение SLA',
        'Нарушение SLA без ожидания клиента',
        'МРФ подключения',
        'РФ подключения'
    ]
    if not all(col in df.columns for col in required_cols):
        await update.message.reply_text("❌ В файле отсутствуют необходимые столбцы.")
        return

    if "dwh" in doc.file_name.lower() or "sla" in doc.file_name.lower():
        df = fix_ott(df)
    else:
        await update.message.reply_text("ℹ️ Имя файла должно содержать 'dwh' или 'sla'.")
        return

    base_mask = (
        (df['"source_NTTM_DB"[3ЛТП_Признак]'] == 1) &
        (df['Исключить ЦЭ'] == 'Без признака ЦЭ') &
        (df['Исключить по услуге'] == 'Расчетные услуги')
    )
    df = df[base_mask].copy()
    if df.empty:
        await update.message.reply_text("ℹ️ После фильтрации данных нет.")
        return

    # =====================================================================
    # Формирование отчёта в текстовом формате
    # =====================================================================

    for mrf, mrf_df in df.groupby(['МРФ подключения']):
        mrf_name = mrf if isinstance(mrf, str) else mrf[0]
        report_lines = [f"📊 Отчёт по SLA (3ЛТП), норматив: 87.0%\n"]
        report_lines.append(f"📍 {mrf_name}\n")

        for rf, group_df in mrf_df.groupby(['РФ подключения']):
            rf_name = rf if isinstance(rf, str) else rf[0]
            report_lines.append(f"📌 {rf_name}\n")

            for level_name, df_level in [("Платина", group_df[group_df['Уровень'] == 'Платиновый']),
                                         ("Прочие", group_df[group_df['Уровень'].isin(['Бронзовый', 'Золотой', 'Серебряный'])])]:
                df_level['Нарушение SLA'] = normalize_sla_column(df_level)
                total = len(df_level)
                on_time = (df_level['Нарушение SLA'] == 0).sum()
                sla_pct, buffer, status = calc_sla(total, on_time)

                report_lines.append(f"SLA 3лтп {level_name}")
                report_lines.append(f"В срок: {on_time}")
                report_lines.append(f"Всего: {total}")
                report_lines.append(f"SLA: {sla_pct}% {status}")
                if buffer < 0:
                    report_lines.append(f"Нужно до норматива: {abs(buffer)}")
                report_lines.append("")  # пустая строка между уровнями

        report_text = "\n".join(report_lines)
        await update.message.reply_text(report_text)


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
