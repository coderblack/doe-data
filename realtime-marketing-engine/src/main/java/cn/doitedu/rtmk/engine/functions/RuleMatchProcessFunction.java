package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.common.interfaces.RuleCalculator;
import cn.doitedu.rtmk.common.interfaces.TimerRuleCalculator;
import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.common.utils.UserEventComparator;
import cn.doitedu.rtmk.engine.pojo.RuleMatchResult;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import cn.doitedu.rtmk.engine.utils.FlinkStateDescriptors;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import groovy.lang.GroovyClassLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/20
 * @Desc: 规则运算、匹配的核心计算函数
 **/
@Slf4j
public class RuleMatchProcessFunction extends KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject> {

    MapState<String, Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        timerState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("timerState", String.class, Long.class));
    }

    /**
     * 处理用户事件流
     */
    @Override
    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        ReadOnlyBroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(FlinkStateDescriptors.ruleMetaBeanMapStateDescriptor);
        Iterable<Map.Entry<String, RuleMetaBean>> ruleEntries = broadcastState.immutableEntries();

        // 遍历每一个规则，进行相应处理
        for (Map.Entry<String, RuleMetaBean> ruleEntry : ruleEntries) {

            // 取出规则的封装对象
            RuleMetaBean ruleMetaBean = ruleEntry.getValue();

            // 调用规则的运算机，对输入事件进行处理
            RuleCalculator ruleCalculator = ruleMetaBean.getRuleCalculator();

            // 如果该规则的运算机是需要用到定时器功能的
            List<JSONObject> caculatorResponse;
            if (ruleCalculator instanceof TimerRuleCalculator) {
                TimerRuleCalculator timerRuleCalculator = (TimerRuleCalculator) ruleCalculator;
                caculatorResponse = timerRuleCalculator.process(userEvent, timerState, ctx.timerService());
            } else {
                caculatorResponse = ruleCalculator.process(userEvent);
            }

            // 输出运算机的响应数据
            for (JSONObject resObject : caculatorResponse) {
                if ("match".equals(resObject.getString("resType"))) {
                    out.collect(resObject);
                } else {
                    ctx.output(new OutputTag<>("ruleStatInfo", TypeInformation.of(JSONObject.class)), resObject);
                }
            }


        }

    }


    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {

        for (Map.Entry<String, Long> entry : timerState.entries()) {
            // ruleId + ":" + checkEventAttributeValue
            String key = entry.getKey();
            String ruleId = key.split(":")[0];
            Long registerTime = entry.getValue();
            if (registerTime != null && registerTime == timestamp) {
                RuleCalculator ruleCalculator = ctx.getBroadcastState(FlinkStateDescriptors.ruleMetaBeanMapStateDescriptor).get(ruleId).getRuleCalculator();
                if (ruleCalculator instanceof TimerRuleCalculator) {

                    TimerRuleCalculator timerRuleCalculator = (TimerRuleCalculator) ruleCalculator;

                    List<JSONObject> resJsonObjects = timerRuleCalculator.onTimer(timestamp, ctx.getCurrentKey(),timerState, ctx.timerService());
                    for (JSONObject resObject : resJsonObjects) {
                        if ("match".equals(resObject.getString("resType"))) {
                            log.info("-----flink---中----输出了");
                            out.collect(resObject);
                        } else {
                            ctx.output(new OutputTag<>("ruleStatInfo", TypeInformation.of(JSONObject.class)), resObject);
                        }
                    }

                }
            }
        }

    }

    /**
     * 处理规则元信息广播流
     * 也就是规则引擎的规则注入模块
     */
    @Override
    public void processBroadcastElement(RuleMetaBean ruleMetaBean, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

        BroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(FlinkStateDescriptors.ruleMetaBeanMapStateDescriptor);

        // 根据收到的规则管理的操作类型，去操作广播状态
        try {
            String operateType = ruleMetaBean.getOperateType();
            if ("INSERT".equals(operateType) || "UPDATE".equals(operateType)) {
                // 把规则的运算机groovy代码，动态编译加载并反射成具体的运算机对象
                GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
                Class aClass = groovyClassLoader.parseClass(ruleMetaBean.getCaculatorGroovyCode());
                RuleCalculator ruleCalculator = (RuleCalculator) aClass.newInstance();

                // 对规则运算器做初始化
                ruleCalculator.init(JSON.parseObject(ruleMetaBean.getRuleParamJson()), ruleMetaBean.getProfileUserBitmap());

                /*if (ruleCalculator instanceof TimerRuleCalculator) {
                    TimerRuleCalculator timerRuleCalculator = (TimerRuleCalculator) ruleCalculator;
                    timerRuleCalculator.setTimeService(null);
                }*/

                // 然后将创建好的运算机对象，填充到ruleMetaBean
                ruleMetaBean.setRuleCalculator(ruleCalculator);

                // 再把ruleMetaBean，放入广播状态
                broadcastState.put(ruleMetaBean.getRuleId(), ruleMetaBean);
                log.debug("收到规则管理信息，操作类型:{}, 规则模型:{},规则id:{} ,创建人:{}", ruleMetaBean.getOperateType(), ruleMetaBean.getRuleModelId(), ruleMetaBean.getRuleId(), ruleMetaBean.getCreatorName());
            } else {
                // 从广播状态中，删除掉该规则的ruleMetaBean
                broadcastState.remove(ruleMetaBean.getRuleId());
                log.debug("收到规则管理信息，操作类型:删除, 规则id:{}", ruleMetaBean.getRuleModelId());
            }
        } catch (Exception e) {
            log.debug("收到规则管理信息，规则信息构建失败: {}", e.getMessage());
        }
    }
}
