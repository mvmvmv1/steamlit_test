with current_wave as
         (select w.number,
                 w.created_date,
                 count(distinct si.id) cnt,
                 count(distinct si.id) filter (where picker_id is null) cnt_not_in_picking,
                 count(distinct si.id) filter (where picker_id is not null) cnt_in_picking
          from wave w
                   join assembly a on w.id = a.wave_id and a.status not in ('COMPLETED', 'CANCELED')
                   join stock s on w.stock_id = s.stock_uuid
                   join sku_item si on a.id = si.assembly_id and si.status = 'RESERVED'
          where w.assembly_type = 'ORDER'
            and s.title = 'Фулфилмент'
          group by 1, 2),
     past_wave as
         (select w.number,
                 w.created_date,
                 count(distinct sih.id) cnt
          from wave w
                   join assembly a on w.id = a.wave_id and a.status not in ('COMPLETED', 'CANCELED')
                   join stock s on w.stock_id = s.stock_uuid
                   join sku_item_history sih on a.id = sih.assembly_id and sih.status = 'RESERVED'
          where w.assembly_type = 'ORDER'
            and s.title = 'Фулфилмент'
            and w.number in (select number from current_wave)
          group by 1, 2)
select cw.created_date as "Дата запуска",
       cw.number as "Номер волны",
       pw.cnt as "Было запущено",
       cw.cnt_not_in_picking + cw.cnt_in_picking as "Осталось отобрать",
       cw.cnt_not_in_picking as "Не назначено",
       cw.cnt_in_picking as "Назначено"
from current_wave cw
join past_wave pw on cw.number = pw.number
order by cw.created_date desc
limit 5