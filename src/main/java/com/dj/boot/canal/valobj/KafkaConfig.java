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
 * @author mason
 * @version 1.0
 * @date 2021/8/19 下午12:06
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class KafkaConfig implements Serializable {
    private String topic;
}
