package cn.doitedu.rtmk.common.interfaces;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.TimerService;

import java.util.List;

/**
 * 需要用到定时器功能规则模型的运算机接口
 */
public abstract class TimerRuleCalculator implements RuleCalculator {

    public abstract List<JSONObject> onTimer(long timestamp, int guid,MapState<String,Long> timerState,TimerService timerService);

    public abstract List<JSONObject> process(UserEvent userEvent,MapState<String,Long> timerState,TimerService timerService);

    @Override
    public void calc(UserEvent userEvent) {

    }

    @Override
    public boolean isMatch(int guid) {
        return false;
    }
}
