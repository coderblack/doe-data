package cn.doitedu.rulemgmt;

import java.util.Arrays;
import java.util.List;

public class SeqMatchTest {

    public static void main(String[] args) {


        String[] split = {"a","a","b","a","c","c","b","a","b"};
        List<String> eventParams = Arrays.asList("a", "b", "c");


        // 条件序列中的比较位置
        int k = 0;
        int matchCount = 0;

        // 遍历查询出来的行为序列
        for (int i = 0; i < split.length; i++) {
            if (split[i].split("_")[0].equals(eventParams.get(k))) {
                k++;
                if (k == eventParams.size()) {
                    k = 0;
                    matchCount++;
                }
            }
        }

        System.out.println("matchCount: " +matchCount);
        System.out.println("k : " + k);


    }


}
