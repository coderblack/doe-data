package cn.doitedu.datacollect.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * 字段脱敏拦截器
 * 本拦截器，要脱敏哪个字段，不是写死的，需要在采集配置中通过参数来指定，如下：
 * desensitive.field = account
 */
public class DesensitizationInterceptor implements Interceptor {
    String desField;

    public DesensitizationInterceptor(String desField) {
        this.desField = desField;
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        try {
            byte[] dataBytes = event.getBody();
            String json = new String(dataBytes);

            JSONObject jsonObject = JSON.parseObject(json);

            // 取到待脱敏字段的原初值
            String originValue = jsonObject.getString(desField);

            // 加密，得到脱敏的密文
            String desensitiveValue = DigestUtils.md5Hex(originValue);

            // 将密文，覆盖掉json中的原初值
            jsonObject.put(desField, desensitiveValue);

            // 把jsonObject变回json字符串
            String desensitiveJson = jsonObject.toJSONString();

            // 将处理好的数据json字符串，替换掉event中原来的数据
            event.setBody(desensitiveJson.getBytes());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    @Override
    public void close() {

    }


    public static class DesensitizationInterceptorBuilder implements Interceptor.Builder {
        String desField;

        @Override
        public Interceptor build() {

            return new DesensitizationInterceptor(desField);
        }

        @Override
        public void configure(Context context) {
            desField = context.getString("desensitive.field");

        }
    }


}
