package cn.fxbin.flink.source;

import cn.fxbin.flink.entity.User;
import cn.fxbin.flink.utils.MysqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * SinkToMysql
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 17:11
 */
public class SinkToMysql extends RichSinkFunction<User> {

    PreparedStatement ps;

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MysqlUtils.getConnection();
        String sql = "INSERT INTO `user`(`id`, `username`, `age`, `create_date`) VALUES (?, ?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();

        // 关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(User user, Context context) throws Exception {

        ps.setInt(1, user.getId());
        ps.setString(2, user.getUsername());
        ps.setInt(3, user.getAge());
        ps.setDate(4, user.getCreateDate());

        ps.executeUpdate();
    }


}
