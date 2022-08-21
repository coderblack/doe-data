package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.common.interfaces.RuleCalculator;
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
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
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
public class RuleMatchProcessFunctionOld extends KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject> {
    private Jedis jedis;

    MapState<Long, List<String>> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("doitedu", 6379);
        super.open(parameters);
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

            // 取出规则的画像人群bitmap
            RoaringBitmap profileUserBitmap = ruleMetaBean.getProfileUserBitmap();

            // 判断本事件的行为人，是否属于本规则的画像人群
            if (profileUserBitmap.contains(userEvent.getGuid())) {

                // 取出规则的参数json
                JSONObject ruleParamJsonObject = JSON.parseObject(ruleMetaBean.getRuleParamJson());

                // 取出规则的触发事件条件参数json
                JSONObject ruleTrigEventJsonObject = ruleParamJsonObject.getJSONObject("ruleTrigEvent");

                // 判断用户行为事件，如果本事件是规则的触发条件，则进行规则的匹配判断
                if (UserEventComparator.userEventIsEqualParam(userEvent, ruleTrigEventJsonObject)) {

                    // 如果是触发事件，则判断本行为人是否已经满足了本规则的所有条件
                    boolean isMatch = ruleMetaBean.getRuleCalculator().isMatch(userEvent.getGuid());

                    log.info("用户:{} ,触发事件:{},规则:{},规则匹配结果:{}",userEvent.getGuid(),userEvent.getEventId(),ruleEntry.getKey(),isMatch);
                    // 如果已满足，则输出本规则的触达信息
                    if(isMatch) {
                        RuleMatchResult res = new RuleMatchResult(userEvent.getGuid(), ruleEntry.getKey(), System.currentTimeMillis());
                        out.collect(JSON.parseObject(JSON.toJSONString(res)));
                    }

                }
                // 判断用户行为事件，如果本事件不是规则的触发事件，则进行规则的条件统计运算
                else {
                    // 做规则运算
                    ruleMetaBean.getRuleCalculator().calc(userEvent);
                    log.info("收到用户:{} ,行为事件:{}, 规则条件运算：{}", userEvent.getGuid(), userEvent.getEventId(), ruleEntry.getKey());
                }
            }
        }

        // 判断该规则，是否需要定时功能，比如（要求发生A后的15分钟内没有发生B，则要输出触达信息）

        // 如果需要定时功能，则在调用规则运算机处理事件时，传入一个 ctx 和一个记录定时注册的状态 timerState
        // 规则运算机中，可以利用ctx做定时注册：
        //   当检测到当前事件是A事件时，注册一个15分钟定时器
        /*ctx.timerService().registerProcessingTimeTimer(17:25);
        List<String> rules = timerState.get(17:25)
        rules.add(ruleId);
        timerState.put(rules);
        //   如果是一个B事件，删除之前的定时
        ctx.timerService().deleteProcessingTimeTimer(17:25);
        List<String> rules = timerState.get(17:25)
        rules.remove(ruleId);
        timerState.put(rules);*/

    }



    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {

        /*ReadOnlyBroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(FlinkStateDescriptors.ruleMetaBeanMapStateDescriptor);

        List<String> rules = timerState.get(timestamp);
        for (String ruleId : rules) {
            RuleMetaBean ruleMetaBean = broadcastState.get(ruleId);
            // 调用规则bean的处理定时到达的功能
            ruleMetaBean.getRuleConditionCalculator().timer()

        }*/


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
                Class aClass = new GroovyClassLoader().parseClass(ruleMetaBean.getCaculatorGroovyCode());
                RuleCalculator ruleConditionCalculator = (RuleCalculator) aClass.newInstance();

                // 对规则运算器做初始化
                ruleConditionCalculator.init(jedis, JSON.parseObject(ruleMetaBean.getRuleParamJson()),ruleMetaBean.getProfileUserBitmap(),out);

                // 然后将创建好的运算机对象，填充到ruleMetaBean
                ruleMetaBean.setRuleCalculator(ruleConditionCalculator);

                // 再把ruleMetaBean，放入广播状态
                broadcastState.put(ruleMetaBean.getRuleId(), ruleMetaBean);
                log.info("接收到一个规则管理信息，操作类型是:{}, 所属的规则模型是:{} ,创建人是:{}", ruleMetaBean.getOperateType(), ruleMetaBean.getRuleModelId(), ruleMetaBean.getCreatorName());
            } else {
                // 从广播状态中，删除掉该规则的ruleMetaBean
                broadcastState.remove(ruleMetaBean.getRuleId());
                log.info("接收到一个规则管理信息，操作类型是:删除, 删除的规则id:{}", ruleMetaBean.getRuleModelId());
            }
        } catch (Exception e) {
            log.info("接收到一个规则管理信息，但规则信息构建失败: {}", e.getMessage());
        }
    }
}
