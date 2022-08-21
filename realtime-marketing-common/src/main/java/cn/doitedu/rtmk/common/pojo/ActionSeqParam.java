package cn.doitedu.rtmk.common.pojo;

/*
 *
 {
 "eventParams": [
 {
 "eventId": "e1",
 "attributeParams": [
 {
 "attributeName": "pageId",
 "compareType": "=",
 "compareValue": "page001"
 }
 ]
 }
 ],
 "windowStart": "2022-08-01 12:00:00",
 "windowEnd": "2022-08-30 12:00:00",
 "conditionId": 3,
 "dorisQueryTemplate": "action_seq",
 "seqCount": 2
 }
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ActionSeqParam {

    private String windowStart;
    private String windowEnd;
    private int conditionId;
    private String dorisQueryTemplate;
    private int seqCount;

    private List<EventParam> eventParams;
}
