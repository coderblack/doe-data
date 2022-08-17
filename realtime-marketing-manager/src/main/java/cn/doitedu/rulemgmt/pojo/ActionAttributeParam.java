package cn.doitedu.rulemgmt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ActionAttributeParam {

    private String attributeName;
    private String compareType;
    private Object compareValue;
}
