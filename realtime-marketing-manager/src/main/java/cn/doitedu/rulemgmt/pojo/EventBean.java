package cn.doitedu.rulemgmt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean {
    private int guid;
    private String eventId;
    private Map<String,String> properties;
    private long eventTime;

}
