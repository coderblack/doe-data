package cn.doitedu.rtmk.tech_test.enjoy_test;

import com.jfinal.kit.Kv;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;

import java.util.ArrayList;
import java.util.HashMap;

public class EnjoyTest {
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



    }
}
