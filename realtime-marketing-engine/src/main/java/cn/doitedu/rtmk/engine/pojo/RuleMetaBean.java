package cn.doitedu.rtmk.engine.pojo;


import cn.doitedu.rtmk.common.interfaces.RuleCalculator;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.roaringbitmap.RoaringBitmap;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/20
 * @Desc:
 *
 * CREATE TABLE `rule_instance_definition` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `rule_id` varchar(50) DEFAULT NULL,
 *   `rule_model_id` int(11) DEFAULT NULL,
 *   `rule_profile_user_bitmap` binary(255) DEFAULT NULL,
 *   `caculator_groovy_code` text,
 *   `creator_name` varchar(255) DEFAULT NULL,
 *   `rule_status` int(11) DEFAULT NULL,
 *   `create_time` datetime DEFAULT NULL,
 *   `update_time` datetime DEFAULT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
 *
 *
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleMetaBean {

    private String operateType; // 规则管理操作类型： 新增，停用 ，更新
    private String ruleId;
    private int ruleModelId;
    private RoaringBitmap profileUserBitmap;
    private String caculatorGroovyCode;
    private String ruleParamJson;
    private String creatorName;
    private int ruleStatus;

    private RuleCalculator ruleCalculator;



}
