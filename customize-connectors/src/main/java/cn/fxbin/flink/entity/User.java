package cn.fxbin.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;


/**
 * User
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 16:48
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class User {

    private Integer id;

    private String username;

    private Integer age;

    private Date createDate;

}
