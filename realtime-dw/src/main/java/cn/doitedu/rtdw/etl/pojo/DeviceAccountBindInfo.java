package cn.doitedu.rtdw.etl.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeviceAccountBindInfo {

    private String deviceId;
    private String account;
    private Float weight;
    private Long userId;
    private Long registerTime;
}
