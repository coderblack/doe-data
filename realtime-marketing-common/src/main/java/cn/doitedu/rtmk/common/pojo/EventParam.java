package cn.doitedu.rtmk.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class EventParam {

     private String eventId;
     private List<AttributeParam> attributeParams;


}
