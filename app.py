"""
app.py — Streamlit дашборд
"""
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from config import EXCEL_DIR, HTML_DIR, MOSCOW_TZ, REPO_URL
from log_manager import LogManager

st.set_page_config(page_title="Мониторинг банкротств", page_icon="🔍", layout="wide")
st.title("🔍 Мониторинг банкротств (Федресурс)")
st.caption(f"Обновлено: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')}")

log_manager = LogManager()

with st.sidebar:
    st.header("⚙️ Настройки")
    days_filter = st.slider("Период истории (дней)", 7, 180, 30)

st.subheader("📋 История запусков")

history = log_manager.get_history(days_filter)

if history.empty:
    st.info("📭 Нет данных за выбранный период. Запустите парсинг.")
else:
    rows = []
    for _, row in history.iterrows():
        excel_link = f"{REPO_URL}/blob/main/logs/excel/{row['excel_path']}"
        html_link = f"{REPO_URL}/blob/main/logs/html/{row['html_path']}"
        status_badge = log_manager.get_status_badge(row['id'])
        
        rows.append({
            'Дата': row['run_date'],
            'Excel': f"[📥 Скачать]({excel_link})",
            'HTML': f"[👁️ Просмотр]({html_link})",
            'Статус': status_badge,
            'Компаний': row['total_companies'],
            'Признаки': row['has_signs_count']
        })
    
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

if not history.empty:
    st.divider()
    st.subheader("📊 Статистика")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Всего запусков", len(history))
    with col2:
        total_changes = sum(1 for _, row in history.iterrows() 
                          if '⚠️' in log_manager.get_status_badge(row['id']))
        st.metric("Запусков с изменениями", total_changes)
    
    st.bar_chart(history.groupby('run_date')[['has_signs_count', 'errors_count']].sum())

st.caption(f"© {datetime.now().year} • Мониторинг Федресурса")