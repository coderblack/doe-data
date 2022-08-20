package cn.doitedu.rtmk.engine.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/20
 * @Desc: 封装规则触达结果的javabean
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleMatchResult {

    private int guid;
    private String ruleId;
    private long matchTime;

}
