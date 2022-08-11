package cn.doitedu.rtdw.etl.pojo;

import lombok.*;

import java.io.Serializable;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2022/4/6
 **/
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class TrafficBean implements Serializable {
    private long guid;
    private String sessionId;
    private String splitSessionId;
    private String eventId;
    private long ts;
    private String pageId;
    private long pageLoadTime;
    private String province;
    private String city;
    private String region;
    private String deviceType;
    private int isNew;
    private String releaseChannel;
}
