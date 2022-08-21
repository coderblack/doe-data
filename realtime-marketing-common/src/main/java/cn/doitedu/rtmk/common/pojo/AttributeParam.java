package cn.doitedu.rtmk.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 "attributeName": "pageId",
 "compareType": "=",
 "compareValue": "page001"
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AttributeParam {
     private String attributeName;
     private String compareType  ;
     private String compareValue ;

}
