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
GROUP BY 1, 2, 3