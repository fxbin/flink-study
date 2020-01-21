package cn.fxbin.flink.contant;

/**
 * PropertiesConstants
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/21 12:03
 */
public interface PropertiesConstants {

    String PROPERTIES_FILE_NAME = "/application.properties";

    String STREAM_PARALLELISM = "stream.parallelism";

    String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";

    String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
}
