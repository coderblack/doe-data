package cn.doitedu.rulemgmt.service;

import cn.doitedu.rulemgmt.dao.RuleSystemMetaDao;
import org.roaringbitmap.RoaringBitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;

@Service
public class RuleSystemMetaServiceImpl implements RuleSystemMetaService {
    RuleSystemMetaDao ruleSystemMetaDao;

    @Autowired
    public RuleSystemMetaServiceImpl(RuleSystemMetaDao ruleSystemMetaDao){
        this.ruleSystemMetaDao = ruleSystemMetaDao;
    }

    /**
     * 根据规则模型的id，查询规则模型的运算代码模板
     * @param ruleModelId
     * @return
     * @throws SQLException
     */
    @Override
    public String findRuleModelGroovyTemplate(int ruleModelId) throws SQLException {

        String template = ruleSystemMetaDao.queryGroovyTemplateByModelId(ruleModelId);

        return template;
    }


    /**
     * 发布新规则到元数据库中
     */
    @Override
    public void publishRuleInstance(
            String rule_id,
            int rule_model_id,
            String creator_name,
            int rule_status,
            RoaringBitmap profileUserBitmap,
            String ruleDefineParamsJson,
            String ruleModelCaculatorGroovyCode
    ) throws IOException, SQLException {

        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        DataOutputStream dao = new DataOutputStream(bao);
        profileUserBitmap.serialize(dao);

        byte[] bitmapBytes = bao.toByteArray();

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());


        ruleSystemMetaDao.insertRuleInfo(rule_id,rule_model_id,creator_name,rule_status,timestamp,timestamp,bitmapBytes,
                ruleDefineParamsJson,ruleModelCaculatorGroovyCode);

    }


}
