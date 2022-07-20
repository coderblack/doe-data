package cn.doitedu.utils;

import ch.hsr.geohash.GeoHash;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.*;

public class GaodeGpsUtil {

    public static void main(String[] args) throws IOException {

        // 构造hdfs的客户端
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://doitedu:8020/");
        FileSystem fs = FileSystem.get(conf);

        // 创建一个http请求客户端
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String requestUrl = "https://restapi.amap.com/v3/geocode/regeo?key=565bbb9a75ad9f030c51b4e42fde3373&location=";

        // 创建一个写出结果数据的hdfs的文件及输出流
        FSDataOutputStream output = fs.create(new Path("/unknown-gps-know/2022-07-16/res.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(output));

        // 列出 日期目录下的  gps待请求清单文件
        RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(new Path("/unknown-gps/2022-07-16/"), false);
        while (filesIterator.hasNext()) {
            LocatedFileStatus file = filesIterator.next();
            if (file.getPath().getName().contains("_SUCCESS")) continue;

            System.out.println("找到一个待处理文件： " + file.getPath());
            // 打开文件得到读取的输入流
            FSDataInputStream inputStream = fs.open(file.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = br.readLine()) != null) {

                try {
                    // 取到一个gps经纬度座标
                    String[] gps = line.split(",");
                    System.out.println("拿到一个gps座标： " + line);
                    String geohash = GeoHash.geoHashStringWithCharacterPrecision(Double.parseDouble(gps[0]), Double.parseDouble(gps[1]), 5);
                    HttpGet get = new HttpGet(requestUrl + gps[1] + "," + gps[0]);

                    // 发出请求
                    CloseableHttpResponse response = httpClient.execute(get);

                    // 从响应中提取出结果json串
                    HttpEntity entity = response.getEntity();
                    String resJson = EntityUtils.toString(entity);
                    JSONObject jsonObject = JSON.parseObject(resJson);

                    // 判断请求状态是否成功
                    Integer status = jsonObject.getInteger("status");
                    if (status == 1) {
                        // 从响应json中解析出 省、市、区
                        JSONObject regeocodes = jsonObject.getJSONObject("regeocode");
                        JSONObject addressComponent = regeocodes.getJSONObject("addressComponent");
                        String province = addressComponent.getString("province");
                        String city = addressComponent.getString("city");
                        String district = addressComponent.getString("district");

                        // 只要得到了省市区，就开始输出结果
                        if (StringUtils.isNotBlank(province)) {
                            System.out.println(geohash + "," + province + "," + city + "," + district);
                            // geohash,province,city,region
                            bw.write(geohash + "," + province + "," + city + "," + district);
                            bw.newLine();
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                }

            }

            br.close();
            inputStream.close();

        }

        bw.close();
        output.close();
        httpClient.close();

    }
}
