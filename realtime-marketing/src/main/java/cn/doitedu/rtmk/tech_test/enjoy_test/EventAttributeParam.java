package cn.doitedu.rtmk.tech_test.enjoy_test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventAttributeParam {
    private String attributeName;
    private String compareType;
    private Object attributeValue;
}
