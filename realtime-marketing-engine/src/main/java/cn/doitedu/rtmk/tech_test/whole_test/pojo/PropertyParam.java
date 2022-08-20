package cn.doitedu.rtmk.tech_test.whole_test.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PropertyParam {

    private String propName;
    private String compareType;
    private String compareValue;

}
