package cn.doitedu.rulemgmt;

import cn.doitedu.rulemgmt.pojo.ActionAttributeParam;
import com.jfinal.kit.Kv;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;

import java.util.Arrays;
import java.util.HashMap;

public class EnjoyHelloWorld {

    public static void main(String[] args) {


        // demo示例1
        /*Template template = Engine.use().getTemplateByString("i am #(name)");
        Kv data = Kv.by("name", "taoge");
        String s = template.renderToString(data);
        System.out.println(s);*/


        // demo示例2
        /*Template template = Engine.use().getTemplateByString("i am #if(name.equals(\"taoge\")) #(name) 本尊  #else #(name) 小可爱 #end");
        Kv data = Kv.by("name", "幺妹");
        String s = template.renderToString(data);
        System.out.println(s);*/

        // demo示例3
        /*String templateStr = "我的学生有：\n" +
                "#for(stu: students)\n" +
                "    学生#(for.count):#(stu)\n" +
                "#end ";
        Template template = Engine.use().getTemplateByString(templateStr);
        Kv data = Kv.by("students", Arrays.asList("阳哥","宇妹","飞哥","幺妹","敏妹"));
        String s = template.renderToString(data);
        System.out.println(s);*/
        /**
         * 我的学生有：
         *     学生1:阳哥
         *     学生2:宇妹
         *     学生3:飞哥
         *     学生4:幺妹
         *     学生5:敏妹
         */

        // demo 示例4
        String sqlTemplateStr = "SELECT\n" +
                "   guid,\n" +
                "   count(1) as cnt\n" +
                "FROM mall_app_events_detail\n" +
                "WHERE 1=1 \n" +
                "#if( windowStart != null )\n" +
                "AND event_time>='#(windowStart)' \n" +
                "#end\n" +
                "#if( windowEnd != null )\n" +
                "AND event_time<='#(windowEnd)'\n" +
                "#end\n" +
                "#if(eventId != null)\n" +
                "AND event_id = '#(eventId)'\n" +
                "#end\n" +
                "#for(attrParam: attrParamList)\n" +
                "AND get_json_string(propJson,'$.#(attrParam.attributeName)') #(attrParam.compareType) '#(attrParam.compareValue)'\n" +
                "#end\n" +
                "GROUP BY guid";

        ActionAttributeParam p1 = new ActionAttributeParam("pageId", "=", "page002");
        ActionAttributeParam p2 = new ActionAttributeParam("itemId", "=", "item002");
        ActionAttributeParam p3 = new ActionAttributeParam("attr2", ">", 3);

        HashMap<String, Object> data2 = new HashMap<>();
        data2.put("eventId","e1");
        data2.put("windowStart","2022-08-01 10:00:00");
        data2.put("windowEnd","2022-08-31 12:00:00");
        data2.put("attrParamList",Arrays.asList(p1));

        Template template2 = Engine.use().getTemplateByString(sqlTemplateStr);
        String sql = template2.renderToString(data2);

        System.out.println(sql);


    }

}
