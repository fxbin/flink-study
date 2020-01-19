package cn.fxbin.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Metric
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 15:41
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Metric {

    private String name;

    private long timestamp;

    private Map<String, Object> fields;

    private Map<String, String> tags;

}
