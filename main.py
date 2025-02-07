import streamlit as st
import pandas as pd
import numpy as np
from itertools import combinations
from clickhouse_driver import Client
import os
from dotenv import load_dotenv
from clickhouse_driver import Client

timeout = 100




# Загружаем переменные из .env
load_dotenv()

def downloading_clickhouse(query_ch, database_ch):
    client = Client(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=os.getenv('CLICKHOUSE_PORT'),
        user=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        connect_timeout=int(os.getenv('CLICKHOUSE_TIMEOUT', '10')),
        send_receive_timeout=int(os.getenv('CLICKHOUSE_TIMEOUT', '10'))
    )
    try:
        result = client.query_dataframe(query_ch)
        return result
    finally:
        if client:
            client.disconnect()


def process_data(df):
    df_initial = df.copy()

    summary_df = df.groupby(["car_sending_sla"]).agg({"number_of_items": "sum"}).reset_index()
    summary_df["routes"] = df.groupby("car_sending_sla")["route_id"].apply(
        lambda x: ", ".join(map(str, x.unique()))).values
    summary_df["zones"] = df.groupby("car_sending_sla").agg({"zone_id": "nunique"}).values
    summary_df["items_to_zones"] = round(summary_df["number_of_items"] / summary_df["zones"], 2)

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


# ClickHouse запрос
database_ch = 'wms'
query_dwh_c = """
WITH sku_item_table AS
         (SELECT *
          FROM silver.sku_item FINAL
          WHERE status = 'RESERVED'),
     cars AS
         (SELECT route_id,
                  min(formatDateTime(toDateTime(car_sending_sla), '%T')) AS car_sending_sla
          FROM golden.dp_dict_extended_hist
          WHERE DATE(dt) = DATE(today()) - INTERVAL '1 day' AND route_id != 0
          GROUP BY 1)
SELECT
        z.id             zone_id,
        cars.car_sending_sla as car_sending_sla,
        a.route_id                                             route_id,
        uniqExact(si.id) number_of_items
FROM sku_item_table si 
         JOIN dict.wms_assembly a ON si.assembly_id = a.id
    AND a.assembly_type = 'ORDER'
    AND a.wave_id IS NULL
         JOIN dict.cell c ON si.place_id = c.cell_id
         JOIN dict.zone z ON z.id = c.zone_id
         JOIN dict.stock s ON z.stock_id = s.id AND s.title = 'Фулфилмент'
         JOIN dict.wms_wms_order wo ON a.source_id = wo.wms_order_id
         JOIN dict.wms_orders o ON toUInt32(o.id) = toUInt32(wo.order_id)
         JOIN cars ON cars.route_id = a.route_id
WHERE DATE(o.date_delivery) = '2025-02-09'
GROUP BY 1, 2, 3;"""

st.title("Waves 🌊🌊🌊 🏄")

# Загружаем данные из ClickHouse при нажатии кнопки
if st.button("Загрузить данные из БД"):
    df = downloading_clickhouse(query_dwh_c, database_ch)
    df_initial, pivot_df, summary_df = process_data(df)

    # Сохраняем данные в session_state
    st.session_state["df_initial"] = df_initial
    st.session_state["pivot_df"] = pivot_df
    st.session_state["summary_df"] = summary_df

# Проверяем, есть ли загруженные данные
if "df_initial" in st.session_state:
    st.write("### Количество заказов по маршрутам")
    st.dataframe(st.session_state["summary_df"])

    unique_times = sorted(st.session_state["summary_df"]["car_sending_sla"].unique())
    selected_times = st.multiselect("Выберите времена отправки", unique_times, default=unique_times)

    # Фильтруем исходный DataFrame по выбранным временам
    filtered_df = st.session_state["df_initial"][
        st.session_state["df_initial"]["car_sending_sla"].isin(selected_times)]

    # Фильтр по количеству товаров
    selected_items_range = st.slider("Выберите диапазон количества товаров", 0, 20000, (0, 20000))

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
            st.dataframe(combinations_df, use_container_width=True, height=600)