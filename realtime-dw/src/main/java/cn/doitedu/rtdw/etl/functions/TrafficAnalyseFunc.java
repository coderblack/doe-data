package cn.doitedu.rtdw.etl.functions;

import cn.doitedu.rtdw.etl.pojo.EventBean;
import cn.doitedu.rtdw.etl.pojo.TrafficBean;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

public class TrafficAnalyseFunc extends KeyedProcessFunction<Tuple2<Long, String>, EventBean, TrafficBean> {


    ValueState<Long> pageLoadTimeState;
    ValueState<Long> splitSessionStartTimeState;
    ValueState<String> pageIdState;

    @Override
    public void open(Configuration parameters) throws Exception {

        // 构造一个state，用于记录当前所在页面的页面打开时间
        ValueStateDescriptor<Long> pageLoadTimeStateDesc = new ValueStateDescriptor<>("pageLoadTime", Long.class);
        pageLoadTimeState = getRuntimeContext().getState(pageLoadTimeStateDesc);


        // 构造一个state，用于记录当前所在切割子会话的起始时间
        ValueStateDescriptor<Long> splitSessionStartTimeStateDesc = new ValueStateDescriptor<>("splitSessionStartTime", Long.class);
        splitSessionStartTimeState = getRuntimeContext().getState(splitSessionStartTimeStateDesc);


        // 构造一个state，用于记录当前所在的页面
        ValueStateDescriptor<String> pageIdStateDesc = new ValueStateDescriptor<>("pageId", String.class);
        pageIdState = getRuntimeContext().getState(pageIdStateDesc);


    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<Tuple2<Long, String>, EventBean, TrafficBean>.Context ctx, Collector<TrafficBean> out) throws Exception {

        // 判断是否新访客
        long firstRegisterTime = eventBean.getRegisterTime();
        long firstAccessTime = eventBean.getFirstAccessTime();

        String firstAccessdate = DateFormatUtils.format(firstRegisterTime > 0 ? firstRegisterTime : firstAccessTime, "yyyy-MM-dd");
        String now = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
        int isNew = firstAccessdate.compareTo(now) >= 0 ? 1 : 0;


        // 判断输入事件，是否是一个app启动事件，或者 “页面起始时间状态”为空
        // 插入本事件的时间到  “页面起始时间状态”  , "切割子会话起始时间状态"
        if ("launch".equals(eventBean.getEventid()) || pageLoadTimeState.value() == null) {
            pageLoadTimeState.update(eventBean.getTimestamp());
            splitSessionStartTimeState.update(eventBean.getTimestamp());
            pageIdState.update(eventBean.getProperties().get("pageId"));
        }


        // 判断本条数据是否是一个pageload事件
        // 如果是，则要输出一条插值虚拟事件
        // 且更新  “页面起始时间状态”
        else if ("pageLoad".equals(eventBean.getEventid())) {
            // 使用上一个页面的时间,输出一个虚拟插值事件
            TrafficBean vitureEvent = new TrafficBean(eventBean.getGuid(),
                    eventBean.getSessionid(),
                    eventBean.getSessionid() + "-" + splitSessionStartTimeState.value(),
                    "viture_event",
                    eventBean.getTimestamp(),
                    pageIdState.value(),
                    pageLoadTimeState.value(),
                    eventBean.getProvince(),
                    eventBean.getCity(),
                    eventBean.getRegion(),
                    eventBean.getDevicetype(),
                    isNew,
                    eventBean.getReleasechannel()
            );
            out.collect(vitureEvent);

            // 更新状态中  "页面起始时间"
            pageLoadTimeState.update(eventBean.getTimestamp());
            // 更新状态中  "当前页面"
            pageIdState.update(eventBean.getProperties().get("pageId"));
        }

        // 判断本条数据是否是一个wakeup事件
        // 需要更新 "切割子会话起始时间"状态  和  "页面起始时间"状态
        else if ("wakeUp".equals(eventBean.getEventid())) {
            splitSessionStartTimeState.update(eventBean.getTimestamp());
            pageLoadTimeState.update(eventBean.getTimestamp());

        }


        // 输出数据
        out.collect(new TrafficBean(eventBean.getGuid(),
                eventBean.getSessionid(),
                eventBean.getSessionid() + "-" + splitSessionStartTimeState.value(),
                eventBean.getEventid(),
                eventBean.getTimestamp(),
                eventBean.getProperties().get("pageId"),
                pageLoadTimeState.value(),
                eventBean.getProvince(),
                eventBean.getCity(),
                eventBean.getRegion(),
                eventBean.getDevicetype(),
                isNew,
                eventBean.getReleasechannel()
        ));

    }
}
