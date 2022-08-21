select
    guid,
    group_concat(concat_ws('_',event_id,event_time))
from mall_app_events_detail
where event_time >= '2022-08-01 12:00:00'
  and event_time <= '2022-08-30 12:00:00'
  and (
        (event_id = 'e1' and get_json_string(propJson,'$.pageId')='page001')
        OR
        (event_id = 'e2' and get_json_string(propJson,'$.itemId')='item002')
        OR
        (event_id = 'e3' and get_json_string(propJson,'$.itemId')='item003')
    )
GROUP BY guid