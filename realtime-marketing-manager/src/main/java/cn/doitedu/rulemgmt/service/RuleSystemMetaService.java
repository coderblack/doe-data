package cn.doitedu.rulemgmt.service;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.sql.SQLException;

public interface RuleSystemMetaService {
    String findRuleModelGroovyTemplate(int ruleModelId) throws SQLException;

    void publishRuleInstance(
            String rule_id,
            int rule_model_id,
            String creator_name,
            int rule_status,
            RoaringBitmap profileUserBitmap,
            String ruleDefineParamsJson,
            String ruleModelCaculatorGroovyCode
    ) throws IOException, SQLException;
}
