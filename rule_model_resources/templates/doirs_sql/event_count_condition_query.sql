SELECT
    guid,
    count(1) as cnt
FROM mall_app_events_detail
WHERE 1=1
    #if( windowStart != null )
AND event_time>='#(windowStart)'
#end
#if( windowEnd != null )
AND event_time<='#(windowEnd)'
#end
#if(eventId != null)
AND event_id = '#(eventId)'
#end
#for(attrParam: attrParamList)
AND get_json_string(propJson,'$.#(attrParam.attributeName)') #(attrParam.compareType) '#(attrParam.compareValue)'
#end
GROUP BY guid