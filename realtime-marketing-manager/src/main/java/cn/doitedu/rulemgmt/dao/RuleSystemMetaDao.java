package cn.doitedu.rulemgmt.dao;

import java.sql.SQLException;
import java.sql.Timestamp;

public interface RuleSystemMetaDao {
    String getSqlTemplateByTemplateName(String conditionTemplateName) throws SQLException;

    String queryGroovyTemplateByModelId(int ruleModelId) throws SQLException;

    boolean insertRuleInfo(String rule_id,
                        int rule_model_id,
                        String creator_name,
                        int rule_status,
                        Timestamp create_time,
                        Timestamp update_time,
                        byte[] bitmapBytes,
                        String ruleDefineParamsJson,
                        String ruleModelCaculatorGroovyCode) throws SQLException;
}
