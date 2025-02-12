import streamlit as st
import pandas as pd
import numpy as np
from itertools import combinations
import psycopg
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

st.set_page_config(layout="wide")


def load_sql_query(filepath, placeholders=None):
    with open(filepath, "r", encoding="utf-8") as file:
        query = file.read()
    if placeholders:
        for key, value in placeholders.items():
            query = query.replace(f"{{{key}}}", str(value))
    return query


def downloading_postgres(query):
    connection = psycopg.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        dbname=os.getenv('POSTGRES_DB'),
        connect_timeout=int(os.getenv('POSTGRES_TIMEOUT', '10'))
    )
    try:
        with connection.cursor() as cursor:
            result = pd.read_sql(query, connection)
            return result
    finally:
        connection.close()


def process_data_wave_data(df):
    df_initial = df.copy()
    summary_df = df.groupby(["car_sending_sla"]).agg({"number_of_items": "sum"}).reset_index()

    summary_df["routes"] = df.groupby("car_sending_sla")["route_id"] \
        .apply(lambda x: ", ".join(map(str, x.unique()))) \
        .reset_index(drop=True)

    summary_df["zones"] = df.groupby("car_sending_sla")["zone_id"].nunique() \
        .reset_index(drop=True)


    pivot_df = df.pivot_table(index=["route_id", "car_sending_sla"],
                              columns="zone_id", values="number_of_items", fill_value=0)

    return df_initial, pivot_df, summary_df


def compute_combinations(route_ids, zone_values):
    combinations_array = []
    for r in range(1, len(route_ids) + 1):
        for combination in combinations(route_ids, r):
            selected_indices = [route_ids.index(route) for route in combination]
            selected_routes = zone_values[selected_indices, :]
            avg_per_zone = round(np.mean(np.sum(selected_routes, axis=0)), 1)
            total_items = np.sum(selected_routes)
            earliest_sla = min(combination, key=lambda x: x[1])[1]
            formatted_routes = ", ".join([str(r[0]) for r in combination])
            combinations_array.append((formatted_routes, avg_per_zone, total_items, earliest_sla))
    return pd.DataFrame(combinations_array, columns=["Routes", "Average Items per Zone", "Total Items", "Earliest SLA"])

st.title("Waves 🌊🌊🌊 🏄")

selected_date = st.date_input("Выберите дату доставки", date.today())

# Загружаем данные из ClickHouse при нажатии кнопки
if st.button("Загрузить данные из БД"):
    wave_query = load_sql_query("sql/next_wave_query.sql", {"delivery_date": selected_date})
    next_wave_df = downloading_postgres(wave_query)
    df_initial, pivot_df, summary_df = process_data_wave_data(next_wave_df)

    # Сохраняем данные в session_state
    st.session_state["df_initial"] = df_initial
    st.session_state["pivot_df"] = pivot_df
    st.session_state["summary_df"] = summary_df

# Проверяем, есть ли загруженные данные
if "df_initial" in st.session_state:
    st.write("### Количество заказов по маршрутам")
    st.dataframe(st.session_state["summary_df"])

    # unique_times = sorted(st.session_state["summary_df"]["car_sending_sla"].unique())
    # selected_times = st.multiselect("Выберите времена отправки", unique_times, default=unique_times)
    #
    # # Фильтр по количеству товаров
    # selected_items_range = st.slider("Выберите диапазон количества товаров", 1, 6000, (2000, 3000))

    unique_times = sorted(st.session_state["summary_df"]["car_sending_sla"].unique())

    # Две колонки
    col1, col2 = st.columns(2)

    with col1:
        selected_times = st.multiselect("Выберите времена отправки", unique_times, default=unique_times)

    with col2:
        selected_items_range = st.slider("Выберите диапазон количества товаров", 1, 6000, (2000, 3000))


    # Фильтруем исходный DataFrame по выбранным временам
    filtered_df = st.session_state["df_initial"][
        st.session_state["df_initial"]["car_sending_sla"].isin(selected_times)]

    # Проверяем, есть ли данные после фильтрации
    if filtered_df.empty:
        st.warning("Нет данных для выбранных времен отправки и диапазона количества товаров.")
    else:
        # Берём route_id и SLA из фильтрованных данных
        route_ids = list(filtered_df[['route_id', 'car_sending_sla']]
                         .drop_duplicates()
                         .itertuples(index=False, name=None))

        # Фильтруем pivot_df по отфильтрованным route_id
        filtered_pivot_df = st.session_state["pivot_df"].loc[
            st.session_state["pivot_df"].index.get_level_values("route_id").isin([r[0] for r in route_ids])
        ]
        zone_values = filtered_pivot_df.to_numpy()

        # Кнопка для расчёта оптимальных маршрутов
        if st.button("Рассчитать оптимальные маршруты"):
            combinations_df = compute_combinations(route_ids, zone_values)

            # Фильтрация по слайдеру количества товаров
            combinations_df = combinations_df[
                (combinations_df["Total Items"] >= selected_items_range[0]) &
                (combinations_df["Total Items"] <= selected_items_range[1])
                ]

            combinations_df = combinations_df.sort_values(by=["Earliest SLA", "Average Items per Zone"],
                                                          ascending=[True, False])

            st.write("### Оптимальные маршруты для запуска")
            st.dataframe(combinations_df, use_container_width=True)