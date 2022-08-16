package cn.doitedu.rtmk.tech_test.whole_test.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleInfo {

    // 规则id
    private String ruleId;

    // 触发条件
    EventCountParam triggerEventCondition;


    // 人群圈选条件
    private List<PropertyParam> profileCondition;

    // 事件次数条件
    private EventCountParam eventCountCondition;

    // 人群圈选bitmap
    private RoaringBitmap profileUsersBitmap;

    // 规则条件运算groovy代码
    private String ruleCaculatorCode;


}
