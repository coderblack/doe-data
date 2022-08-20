package cn.doitedu.rtmk.engine.functions;

import cn.doitedu.rtmk.common.pojo.UserEvent;
import cn.doitedu.rtmk.engine.pojo.RuleMetaBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/8/20
 * @Desc:
 *
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `rule_id` varchar(50) DEFAULT NULL,
 *   `rule_model_id` int(11) DEFAULT NULL,
 *   `rule_profile_user_bitmap` binary(255) DEFAULT NULL,
 *   `caculator_groovy_code` text,
 *   `creator_name` varchar(255) DEFAULT NULL,
 *   `rule_status` int(11) DEFAULT NULL,
 *   `create_time` datetime DEFAULT NULL,
 *   `update_time` datetime DEFAULT NULL,
 *
 **/
public class Row2RuleMetaBeanMapFunction implements MapFunction<Row, RuleMetaBean> {
    @Override
    public RuleMetaBean map(Row row) throws Exception {

        RuleMetaBean ruleMetaBean = new RuleMetaBean();

        if(row.getKind() == RowKind.DELETE){
            ruleMetaBean.setOperateType("DELETE");

            String ruleId = row.getFieldAs("rule_id");
            ruleMetaBean.setRuleId(ruleId);

        }else if(row.getKind() == RowKind.UPDATE_AFTER){
            ruleMetaBean.setOperateType("UPDATE");
            setRuleMetaBeanAttributes(ruleMetaBean,row);

        } else if (row.getKind() == RowKind.INSERT) {
            ruleMetaBean.setOperateType("INSERT");
            setRuleMetaBeanAttributes(ruleMetaBean,row);
        }else{
            return null;
        }

        return ruleMetaBean;
    }


    public void setRuleMetaBeanAttributes(RuleMetaBean ruleMetaBean,Row row) throws IOException {
        String ruleId = row.getFieldAs("rule_id");
        int ruleModelId = row.getFieldAs("rule_model_id");
        byte[] bitmapBytes = row.getFieldAs("rule_profile_user_bitmap");
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));

        String caculatorGroovyCode = row.getFieldAs("caculator_groovy_code");
        String ruleParamJson = row.getFieldAs("rule_param_json");


        String creatorName = row.getFieldAs("creator_name");
        int ruleStatus = row.getFieldAs("rule_status");

        ruleMetaBean.setRuleId(ruleId);
        ruleMetaBean.setRuleModelId(ruleModelId);
        ruleMetaBean.setProfileUserBitmap(bitmap);
        ruleMetaBean.setCaculatorGroovyCode(caculatorGroovyCode);
        ruleMetaBean.setRuleParamJson(ruleParamJson);
        ruleMetaBean.setCreatorName(creatorName);
        ruleMetaBean.setRuleStatus(ruleStatus);
    }

}
