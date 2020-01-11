package cn.fxbin.flink.source;

import cn.fxbin.flink.entity.User;
import cn.fxbin.flink.utils.MysqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;

/**
 * FlinkMysqlSource
 *
 * @author fxbin
 * @version v1.0
 * @since 2020/1/11 16:54
 */
public class SourceFromMysql extends RichSourceFunction<User> {

    PreparedStatement ps;

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MysqlUtils.getConnection();
        String sql = "select * from `user`;";
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
    public void run(SourceContext<User> ctx) throws Exception {

        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            User user = User.builder()
                    .id(resultSet.getInt("id"))
                    .username(resultSet.getString("username"))
                    .age(resultSet.getInt("age"))
                    .createDate(resultSet.getDate("create_date"))
                    .build();
            ctx.collect(user);
        }
    }

    @Override
    public void cancel() {

    }


}
