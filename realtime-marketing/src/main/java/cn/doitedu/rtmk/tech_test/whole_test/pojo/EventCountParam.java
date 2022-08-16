package cn.doitedu.rtmk.tech_test.whole_test.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Base64;
import java.util.List;


@Data
@ToString
public class EventCountParam {

    private String eventId;
    private int count;
    private List<PropertyParam> propertyParams;
    private String windowStart;
    private String windowEnd;
    private String paramId;

}
