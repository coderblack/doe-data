package cn.doitedu.rtdw.etl.functions;

import cn.doitedu.rtdw.etl.pojo.EventBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToEventBeanMapFunction implements MapFunction<String, EventBean> {

    @Override
    public EventBean map(String jsonLine) throws Exception {

        EventBean eventBean = null;

        try{
            eventBean = JSON.parseObject(jsonLine,EventBean.class);
        }catch (Exception e){
            e.printStackTrace();
        }

        return eventBean;
    }
}
