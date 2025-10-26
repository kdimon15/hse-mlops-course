import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

st.set_page_config(page_title="Fraud Detection Dashboard")

@st.cache_resource
def get_engine():
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'fraud_user')
    password = os.getenv('POSTGRES_PASSWORD', 'fraud_pass')
    database = os.getenv('POSTGRES_DB', 'fraud_db')
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)

def get_fraud_transactions():
    engine = get_engine()
    query = """
        SELECT transaction_id, score, fraud_flag, created_at
        FROM transactions
        WHERE fraud_flag = 1
        ORDER BY created_at DESC
        LIMIT 10
    """
    return pd.read_sql(query, engine)

def get_recent_scores(limit):
    engine = get_engine()
    query = f"""
        SELECT score
        FROM transactions
        ORDER BY created_at DESC
        LIMIT {limit}
    """
    return pd.read_sql(query, engine)

st.title("Fraud Detection Dashboard")

if st.button("Посмотреть результаты"):
    st.write("### 10 последних фродовых транзакций")
    fraud_df = get_fraud_transactions()
    
    if len(fraud_df) > 0:
        st.dataframe(fraud_df)
    else:
        st.info("Фродовых транзакций пока нет")
    
    st.write("### Распределение скоров")
    
    num_records = st.slider("Количество последних записей:", min_value=10, max_value=500, value=100, step=10)
    
    scores_df = get_recent_scores(num_records)
    
    if len(scores_df) > 0:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(scores_df['score'], bins=30, color='steelblue', edgecolor='black', alpha=0.7)
        ax.set_xlabel('Скор', fontsize=12)
        ax.set_ylabel('Количество транзакций', fontsize=12)
        ax.set_title(f'Распределение скоров ({len(scores_df)} транзакций)', fontsize=14)
        ax.grid(axis='y', alpha=0.3)
        st.pyplot(fig)
    else:
        st.info("Данных для отображения пока нет")

