package cn.doitedu.datacollect.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class TimestampExtractInterceptor implements Interceptor {

    String timeField;
    public TimestampExtractInterceptor(String timeField){
        this.timeField = timeField;
    }


    /**
     * 初始化方法：当拦截器类被实例化后，会调用一次的方法
     */
    @Override
    public void initialize() {
        // 比如，创建一个mysql连接
    }

    /**
     * 拦截器的核心功能方法
     * 逐条拦截
     * @param event 从source得到的一条数据
     * @return 处理过后的数据
     */
    @Override
    public Event intercept(Event event) {

        // 要从event中拿到日志json字符串

        // 从json字符串中，根据配置参数中的 timeField （时间字段名） ,去抽取时间戳

        // 将时间戳，放入event的 headers中

        // 返回 event

        return null;
    }

    /**
     * 拦截器的核心功能方法
     * 批次拦截
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    /**
     * 做一些退出之前的资源清理工作
     */
    @Override
    public void close() {
        // 比如，关闭数据库连接、关闭文件流
    }

    public static class TimeExtractInterceptorBuilder implements Interceptor.Builder{

        String timeField;

        /**
         * builder的功能所在：帮助构建拦截器类的实例对象
         * @return
         */
        @Override
        public Interceptor build() {

            return new TimestampExtractInterceptor(timeField);
        }

        /**
         * 配置功能
         * 它会接收到flume agent传入的 上下文对象：context
         * 而 context中就包含这采集配置文件中的所有参数
         *
         *   如：
         *  配置文件中，会配置参数：
         *     time.filed = timeStamp
         * @param context
         */
        @Override
        public void configure(Context context) {
            timeField = context.getString("time.field");

        }
    }



}
