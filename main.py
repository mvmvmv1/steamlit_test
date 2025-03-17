import streamlit as st
import pandas as pd
import numpy as np
from itertools import combinations
import psycopg
import os
from dotenv import load_dotenv
from datetime import date
import matplotlib.pyplot as plt
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
            q25 = round(np.percentile(np.sum(selected_routes, axis=0), 25), 1)
            earliest_sla = min(combination, key=lambda x: x[1])[1]
            formatted_routes = ", ".join([str(r[0]) for r in combination])
            combinations_array.append((formatted_routes, total_items, avg_per_zone, q25, earliest_sla))

    return  pd.DataFrame(combinations_array, columns=["Routes", "Total Items", "Average Items per Zone", "Q25", "Earliest SLA"])


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

    active_workers = load_sql_query("sql/active_workers_query.sql")
    active_workers_df = downloading_postgres(active_workers)

    st.session_state["active_workers_df"] = active_workers_df

    wave_history = load_sql_query("sql/wave_history.sql")
    wave_history_df = downloading_postgres(wave_history)

    st.session_state["wave_history_df"] = wave_history_df

# Проверяем, есть ли загруженные данные
if "df_initial" in st.session_state:

    st.write("### Количество заказов по маршрутам")
    st.dataframe(st.session_state["summary_df"], use_container_width=True)

    with st.expander("Прошлые волны"):
        col1, col2 = st.columns(2)
        with col1:
            st.write("### Прошлые волны")
            st.dataframe(st.session_state["wave_history_df"], use_container_width=True)
        with col2:
            st.write("### Число работников")
            st.dataframe(st.session_state["active_workers_df"], use_container_width=True)

    unique_times = sorted(st.session_state["summary_df"]["car_sending_sla"].unique())

    # Две колонки
    col3, col4 = st.columns(2)
    with col3:
        selected_times = st.multiselect("Выберите времена отправки", unique_times, default=unique_times)
    with col4:
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

            combinations_df = combinations_df.sort_values(by = "Average Items per Zone", ascending=False)

            st.session_state["combinations_df"] = combinations_df

            col5, col6 = st.columns(2)
            with col5:
                st.write("### Оптимальные маршруты для запуска")
                st.dataframe(st.session_state["combinations_df"], use_container_width=True)
            with col6:
                # Получаем данные
                top_routes_str = st.session_state["combinations_df"].iloc[0]["Routes"]
                top_routes = [int(route.strip()) for route in top_routes_str.split(",")]
                st.write(f"### Распределение товаров по зонам для маршрутов {top_routes}")
                barplot_df = \
                    st.session_state["df_initial"].query("route_id in @top_routes").groupby('zone_id', as_index=False)[
                        'number_of_items'].sum()

                st.session_state["barplot_df"] = barplot_df

                # Рассчитываем статистики
                median_val = np.median(barplot_df["number_of_items"])
                q25_val = np.percentile(barplot_df["number_of_items"], 25)

                # Строим гистограмму
                plt.figure(figsize=(8, 4))  # Уменьшенный размер графика
                bins = range(0, int(barplot_df["number_of_items"].max()) + 10, 10)  # Бины шагом 10
                plt.hist(barplot_df["number_of_items"], bins=bins, color="blue", alpha=0.6, label="Зоны")

                # Добавляем вертикальные линии для медианы и Q25
                plt.axvline(median_val, color="red", linestyle="dashed", linewidth=2,
                            label=f"Медиана: {median_val:.1f}")
                plt.axvline(q25_val, color="green", linestyle="dashed", linewidth=2, label=f"Q25: {q25_val:.1f}")

                # Настройки графика
                plt.xlabel("Number of Items")
                plt.ylabel("Count of Zones")
                plt.title(f"Распределение количества товаров по зонам")
                plt.grid(axis='y', linestyle='--', alpha=0.7)
                plt.legend()

                # Показываем график
                st.pyplot(plt)