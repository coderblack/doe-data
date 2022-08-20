package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.common.interfaces.RuleConditionCalculator;
import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.engine.pojo.RuleMatchResult;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import cn.doitedu.rtmk.engine.utils.FlinkStateDescriptors;
import groovy.lang.GroovyClassLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.Map;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/20
 * @Desc: 规则运算、匹配的核心计算函数
 **/
@Slf4j
public class RuleMatchProcessFunction extends KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, RuleMatchResult> {

    /**
     * 处理用户事件流
     */
    @Override
    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, RuleMatchResult>.ReadOnlyContext ctx, Collector<RuleMatchResult> out) throws Exception {

        ReadOnlyBroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(FlinkStateDescriptors.ruleMetaBeanMapStateDescriptor);
        Iterable<Map.Entry<String, RuleMetaBean>> ruleEntries = broadcastState.immutableEntries();

        for (Map.Entry<String, RuleMetaBean> ruleEntry : ruleEntries) {
            String ruleId = ruleEntry.getKey();
            RuleMetaBean ruleMetaBean = ruleEntry.getValue();
            RoaringBitmap profileUserBitmap = ruleMetaBean.getProfileUserBitmap();

            log.info("用户:{},事件:{},是否本规则的画像人群:{},被规则:{} ,的运算机处理了:{}", userEvent.getGuid(), userEvent.getEventId(), profileUserBitmap.contains(userEvent.getGuid()), ruleId, ruleMetaBean.getRuleConditionCalculator() != null);
        }


    }

    /**
     * 处理规则元信息广播流
     */
    @Override
    public void processBroadcastElement(RuleMetaBean ruleMetaBean, KeyedBroadcastProcessFunction<Integer, UserEvent, RuleMetaBean, RuleMatchResult>.Context ctx, Collector<RuleMatchResult> out) throws Exception {

        BroadcastState<String, RuleMetaBean> broadcastState = ctx.getBroadcastState(FlinkStateDescriptors.ruleMetaBeanMapStateDescriptor);

        // 根据收到的规则管理的操作类型，去操作广播状态
        try {
            String operateType = ruleMetaBean.getOperateType();
            if ("INSERT".equals(operateType) || "UPDATE".equals(operateType)) {
                // 把规则的运算机groovy代码，动态编译加载并反射成具体的运算机对象
                Class aClass = new GroovyClassLoader().parseClass(ruleMetaBean.getCaculatorGroovyCode());
                RuleConditionCalculator ruleConditionCalculator = (RuleConditionCalculator) aClass.newInstance();

                // 然后将创建好的运算机对象，填充到ruleMetaBean
                ruleMetaBean.setRuleConditionCalculator(ruleConditionCalculator);

                // 再把ruleMetaBean，放入广播状态
                broadcastState.put(ruleMetaBean.getRuleId(), ruleMetaBean);
                log.info("接收到一个规则管理信息，操作类型是:{}, 所属的规则模型是:{} ,创建人是:{}", ruleMetaBean.getOperateType(), ruleMetaBean.getRuleModelId(), ruleMetaBean.getCreatorName());
            } else {
                // 从广播状态中，删除掉该规则的ruleMetaBean
                broadcastState.remove(ruleMetaBean.getRuleId());
                log.info("接收到一个规则管理信息，操作类型是:删除, 删除的规则id:{}", ruleMetaBean.getRuleModelId());
            }
        }catch (Exception e){
            log.info("接收到一个规则管理信息，但规则信息构建失败: {}", e.getMessage());
        }
    }
}
