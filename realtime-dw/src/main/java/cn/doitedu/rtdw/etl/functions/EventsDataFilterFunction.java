package cn.doitedu.rtdw.etl.functions;

import cn.doitedu.rtdw.etl.pojo.EventBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;

public class EventsDataFilterFunction implements FilterFunction<EventBean> {
    @Override
    public boolean filter(EventBean bean) throws Exception {
        return bean!=null && StringUtils.isNotBlank(bean.getDeviceid()) && StringUtils.isNotBlank(bean.getSessionid());
    }
}
