package cn.fxbin.flink.utils;

import lombok.experimental.UtilityClass;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * MysqlUtils
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 17:12
 */
@UtilityClass
public class MysqlUtils {

    /**
     * getConnection
     *
     * @author fxbin
     * @since 2020/1/11 16:58
     * @return java.sql.Connection
     */
    public Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://20.20.0.36:3306/auth_center?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=round&serverTimezone=Asia/Shanghai&&allowMultiQueries=true", "root", "Zichan360!");
        } catch (Exception e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }

}
