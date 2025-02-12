WITH cars AS (
    SELECT * FROM (
        VALUES
            (1, '07:00:00'), (2, '07:00:00'), (3, '07:00:00'), (4, '07:00:00'),
            (5, '07:00:00'), (6, '07:00:00'), (7, '07:00:00'), (8, '07:00:00'),
            (9, '07:00:00'), (10, '07:00:00'), (11, '11:00:00'), (12, '11:00:00'),
            (13, '11:00:00'), (14, '11:00:00'), (15, '11:00:00'), (16, '11:00:00'),
            (17, '11:00:00'), (18, '11:00:00'), (31, '12:00:00'), (32, '02:30:00'),
            (33, '01:00:00'), (34, '07:00:00'), (35, '03:00:00'), (37, '08:00:00'),
            (43, '01:00:00'), (44, '12:00:00'), (45, '12:00:00'), (46, '00:00:00'),
            (53, '12:00:00'), (54, '01:00:00'), (59, '12:00:00'), (60, '12:00:00')
    ) AS tmp(route_id, car_sending_sla)
)
SELECT
    z.id AS zone_id,
    cars.car_sending_sla car_sending_sla,
    a.route_id route_id,
    count(distinct si.id) AS number_of_items
FROM orders o
    JOIN delivery d ON o.delivery_id = d.id AND d.date::date = '{delivery_date}'
    JOIN wms_order wo ON o.id = wo.b2c_order_id
    JOIN assembly a ON wo.wms_order_id = a.source_id AND a.wave_id IS NULL
    join stock s on a.stock_id = s.stock_uuid and s.title = 'Фулфилмент'
    JOIN sku_item si ON a.id = si.assembly_id AND si.status = 'RESERVED'
    JOIN cell c ON si.place_id = c.cell_id
    JOIN zone z ON z.id = c.zone_id
    LEFT JOIN cars ON cars.route_id = a.route_id
WHERE o.date_created >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1,2,3;