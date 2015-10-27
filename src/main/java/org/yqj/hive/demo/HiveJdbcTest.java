package org.yqj.hive.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HiveJdbcTest {

    private static final String DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String []args){
        System.out.println("hive jdbc test content");
        try{
            Class.forName(DRIVERNAME);
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("jdbc class driver load error");
            System.exit(1);
        }
        try{
            Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default","yaoqijun","123456");
            String sql = "select * from user_test";
//            sql ="load data LOCAL inpath '/Users/yaoqijun/Downloads/cache/part-m-00000' into table user_test";
            Statement statement = connection.createStatement();
//            System.out.println(statement.execute(sql));

            ResultSet resultSet = statement.executeQuery(sql);
            int count = 0;
            while (resultSet.next()){
                //System.out.println(resultSet.getString(1)+"--"+resultSet.getString(2)+"--"+resultSet.getString(3));
                count++;
            }
            System.out.println(count/6);

        }catch (Exception e){
            e.printStackTrace();
            System.out.println("load hive data exception");
        }
    }
}
