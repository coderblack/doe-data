package cn.doitedu.rtmk.tech_test.enjoy_test;

import com.jfinal.template.Engine;
import com.jfinal.template.Template;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class EnjoyHello {
    public static void main(String[] args) {
        /*Template template = Engine.use().getTemplateByString("a b c #(id)");
        HashMap<String, Object> data = new HashMap<>();
        data.put("id","1");
        String res = template.renderToString(data);
        System.out.println(res);*/


        Template template = Engine.use().getTemplateByString("#for(x:conditions) \n" +
                "boolean res1 = calc(#(x)); \n" +
                "#end\n" +
                "\n" +
                "boolean res = #(exp)");

        HashMap<String, Object> data = new HashMap<>();
        ArrayList<String> conditions = new ArrayList<>();
        conditions.add("a");
        conditions.add("b");
        conditions.add("c");
        conditions.add("d");

        data.put("conditions",conditions);
        data.put("exp","res1 && ( res2 || res3 ) && res4");

        String res = template.renderToString(data);
        System.out.println(res);

        System.out.println("----------------------");
        Template template2 = Engine.use().getTemplateByString("SELECT\n" +
                "   guid,\n" +
                "   count(1) as cnt\n" +
                "FROM mall_app_events_detail\n" +
                "WHERE 1=1 \n" +
                "#if(windowStart != null)\n" +
                "AND envet_time>='#(windowStart)' \n" +
                "#end\n" +
                "\n" +
                "#if(windowEnd != null)\n" +
                "AND envet_time<=\"#(windowEnd)\"\n" +
                "#end\n" +
                "AND event_id = #(eventId) \n" +
                "#for(x : attributeParams)\n" +
                "AND get_json_string(propJson,'$.#(x.attributeName)') #(x.compareType) '#(x.attributeValue)'\n" +
                "#end\n" +
                "GROUP BY guid");
        HashMap<String, Object> data2 = new HashMap<>();
        data2.put("windowStart","2022-08-01 00:00:00");
        data2.put("windowEnd","2022-08-31 12:00:00");
        data2.put("eventId","addcart");
        data2.put("attributeParams", Arrays.asList(new EventAttributeParam("p1","=","v1"),new EventAttributeParam("p2",">",3)));

        System.out.println(template2.renderToString(data2));

    }
}


