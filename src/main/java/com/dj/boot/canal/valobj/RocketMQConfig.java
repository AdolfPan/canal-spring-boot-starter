package com.dj.boot.canal.valobj;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * <br>
 * <p></p>
 *
 * <br>
 *
 * @author panrusen
 * @version 1.0
 * @date 2021/8/19 下午12:06
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class RocketMQConfig implements Serializable {
    private String namespace = "";
    private String nameServers;
    private String topic;
    private String groupId;
    private boolean trace;
    private String accessKey;
    private String secretKey;
    private String accessChannel;
    private boolean flat;
}
