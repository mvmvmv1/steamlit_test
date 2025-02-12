WITH intervals AS (
    SELECT
        date_trunc('hour', sih.updated_date) + interval '30 minutes' * (EXTRACT(minute FROM sih.updated_date) / 30)::int AS interval_start,
        COUNT(DISTINCT sih.picker_id) AS pickers_cnt,
        COUNT(DISTINCT sih.id) AS items_cnt
    FROM sku_item_history sih
    JOIN assembly a ON sih.assembly_id = a.id
    JOIN stock s ON a.stock_id = s.stock_uuid
    WHERE sih.status = 'PICKED'
        AND sih.updated_date > '2025-02-11'
        AND s.title = 'Фулфилмент'
    GROUP BY interval_start)
SELECT *
FROM intervals
ORDER BY interval_start DESC
offset 1
limit 5