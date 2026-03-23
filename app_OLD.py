"""
Streamlit дашборд для просмотра результатов
"""
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from config import DASHBOARD_CONFIG, EXCEL_DIR, HTML_DIR, MOSCOW_TZ
from log_manager import LogManager

st.set_page_config(
    page_title=" Мониторинг банкротств",
    page_icon="🔍",
    layout="wide"
)

st.title(f"{DASHBOARD_CONFIG['title']}")
st.caption(f"Часовой пояс: {DASHBOARD_CONFIG['timezone']} • Обновлено: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S MSK')}")

log_manager = LogManager()

with st.sidebar:
    st.header(" Настройки")
    days_filter = st.slider(" Период истории (дней)", 7, 180, 30)

st.subheader(" История запусков")
history = log_manager.get_history(days_filter)

if history.empty:
    st.info(" Нет данных за выбранный период. Запустите парсинг.")
else:
    for _, row in history.iterrows():
        with st.expander(f"📅 {row['run_date']} | {row['total_companies']} компаний | Статус: {row['status']}"):
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric(" Нет данных", row['no_data_count'])
            with col2: st.metric(" Есть признаки", row['has_signs_count'])
            with col3: st.metric(" Ошибки", row['errors_count'])
            with col4: st.metric("Время", row['run_timestamp'].split('T')[1][:8] if 'T' in row['run_timestamp'] else "N/A")
            
            files = log_manager.get_file_paths(row['id'])
            col_dl1, col_dl2 = st.columns(2)
            
            if files.get('excel') and os.path.exists(files['excel']):
                with open(files['excel'], "rb") as f:
                    col_dl1.download_button("📥 Скачать Excel", data=f, file_name=row['excel_path'], mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_excel_{row['id']}")
            
            if files.get('html') and os.path.exists(files['html']):
                with open(files['html'], "r", encoding="utf-8") as f:
                    html_content = f.read()
                col_dl2.download_button("📥 Скачать HTML", data=html_content, file_name=row['html_path'], mime="text/html", key=f"dl_html_{row['id']}")
                with st.popover(" Предпросмотр"):
                    st.components.v1.html(html_content, height=400, scrolling=True)

if not history.empty:
    st.divider()
    st.subheader(" Статистика")
    daily_stats = history.groupby('run_date')[['no_data_count', 'has_signs_count', 'errors_count']].sum()
    st.bar_chart(daily_stats)

st.divider()
st.caption(f"© {datetime.now().year} • Автоматический мониторинг Федресурса")
