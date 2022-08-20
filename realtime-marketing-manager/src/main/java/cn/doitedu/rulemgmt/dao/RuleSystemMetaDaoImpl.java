package cn.doitedu.rulemgmt.dao;

import org.springframework.stereotype.Repository;

import java.sql.*;

@Repository
public class RuleSystemMetaDaoImpl implements RuleSystemMetaDao {

    Connection conn;
    public RuleSystemMetaDaoImpl() throws SQLException {
        conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk?useUnicode=true&characterEncoding=utf8", "root", "root");
    }

    @Override
    public String getSqlTemplateByTemplateName(String conditionTemplateName) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("select template_sql  from condition_doris_sql_template where template_name = ?");
        preparedStatement.setString(1,conditionTemplateName);

        ResultSet resultSet = preparedStatement.executeQuery();
        String template_sql = null;
        while(resultSet.next()){
            template_sql = resultSet.getString("template_sql");
        }

        return template_sql;
    }

    @Override
    public String queryGroovyTemplateByModelId(int ruleModelId) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("select caculator_groovy_template  from rulemodel_calculator_templates where rule_model_id = ? and status=1");
        preparedStatement.setInt(1,ruleModelId);

        ResultSet resultSet = preparedStatement.executeQuery();
        String groovyTemplate = null;
        while(resultSet.next()){
            groovyTemplate = resultSet.getString("caculator_groovy_template");
        }

        return groovyTemplate;
    }


    /**
     * CREATE TABLE `rule_instance_definition` (
     *   `id` int(11) NOT NULL AUTO_INCREMENT,
     *   `rule_id` int(11) DEFAULT NULL,
     *   `rule_model_id` int(11) DEFAULT NULL,
     *   `rule_profile_user_bitmap` binary(255) DEFAULT NULL,
     *   `caculator_groovy_code` text,
     *   `creator_name` varchar(255) DEFAULT NULL,
     *   `rule_status` int(11) DEFAULT NULL,
     *   `create_time` datetime DEFAULT NULL,
     *   `update_time` datetime DEFAULT NULL,
     *   PRIMARY KEY (`id`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
     * @param bitmapBytes
     * @param ruleDefineParamsJson
     * @param ruleModelCaculatorGroovyCode
     */
    @Override
    public boolean insertRuleInfo(String rule_id,
                               int rule_model_id,
                               String creator_name,
                               int rule_status,
                               Timestamp create_time,
                               Timestamp update_time,
                               byte[] bitmapBytes,
                               String ruleDefineParamsJson,
                               String ruleModelCaculatorGroovyCode) throws SQLException {

        PreparedStatement preparedStatement = conn.prepareStatement(
                "insert into rule_instance_definition (rule_id,rule_model_id,rule_profile_user_bitmap,caculator_groovy_code,rule_param_json,creator_name,rule_status,create_time,update_time)  values (?,?,?,?,?,?,?,?,?)");

        preparedStatement.setString(1,rule_id);
        preparedStatement.setInt(2,rule_model_id);
        preparedStatement.setBytes(3,bitmapBytes);
        preparedStatement.setString(4,ruleModelCaculatorGroovyCode);
        preparedStatement.setString(5,ruleDefineParamsJson);
        preparedStatement.setString(6,creator_name);
        preparedStatement.setInt(7,rule_status);
        preparedStatement.setTimestamp(8,create_time);
        preparedStatement.setTimestamp(9,update_time);


        boolean execute = preparedStatement.execute();
        return execute;
    }


}
