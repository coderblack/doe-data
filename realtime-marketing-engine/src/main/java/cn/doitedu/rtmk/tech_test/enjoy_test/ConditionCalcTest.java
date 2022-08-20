package cn.doitedu.rtmk.tech_test.enjoy_test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class ConditionCalcTest {
    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {


        String conditionJson = FileUtils.readFileToString(new File("param_jsons/simpleCondition.json"),"utf-8");
        Template template = Engine.use().getTemplateByString(FileUtils.readFileToString(new File("groovy_templates/ConditionCalculator.template"),"utf-8"));

        JSONObject jsonObject = JSON.parseObject(conditionJson);
        int eventParams = jsonObject.getJSONArray("eventParams").size();


        int[] conditions = new int[eventParams];
        HashMap<String, Object> data = new HashMap<>();

        data.put("conditions",conditions);
        data.put("exp","res0 && ( res1 || res2 )");

        String groovyCode = template.renderToString(data);
        //System.out.println(groovyCode);

        Class aClass = new GroovyClassLoader().parseClass(groovyCode);
        IConditionCalculator caculator = (IConditionCalculator) aClass.newInstance();
        caculator.init(conditionJson);

        HashMap<String, String> props = new HashMap<>();
        props.put("p1","2");
        EventBean eventBean = new EventBean("e4", props);
        boolean res = caculator.calc(eventBean);

        System.out.println(res);

    }

}
