package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;

public class Json2UserEventMapFunction implements MapFunction<String, UserEvent> {
    @Override
    public UserEvent map(String eventJson) throws Exception {
        UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);
        return userEvent;
    }
}
