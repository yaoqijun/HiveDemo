package org.yqj.hive.demo;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by yaoqijun.
 * Date:2015-10-29
 * Email:yaoqj@terminus.iove
 * Descirbe:
 */
public class TestContent {
    public static void main(String []args){
        System.out.println("test hive content");
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("terminus-hive-context.xml");
//        HiveTemplate hiveTemplate = context.getBean(HiveTemplate.class);
        JdbcTemplate template = context.getBean(JdbcTemplate.class);
//        List<Map<String,Object>> result = template.queryForList("select * from user_test");
//        System.out.println(result.toString());

        String sql = "";
        //  test get database info, get tables info
//        List<String> result = hiveTemplate.query("show tables");
        //List<Map<String,Object>> result = template.queryForList("show tables");
        //System.out.println(result.toString());
        //List<Map<String,Object>> result = template.queryForList("show databases");
        //System.out.println(result.toString());

        //test table execute
        //sql = " create table test_create(id int, name string, age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ':'";
        //template.execute(sql);
        //sql = "drop table test_create";
        //template.execute(sql);

        //hiveOperations.executeScript(new HiveScript());
//        if(hiveOperations == null){
//            System.out.println("empty");
//            sql = "load data inpath '/user/yaoqijun/part-m-00000' into table test_create";
//            hiveOperations.query(sql);
//        }
        //load data
//        sql = "load data inpath '/user/yaoqijun/part-m-00000' into table throw_test2";
//        template.execute(sql);

//        sql = "insert overwrite table throw_test select * from throw_test where 1=0";
//        template.execute(sql);

        //test insert haha
        sql = "insert into table throw_test VALUES(66,'haha',88)";
        template.execute(sql);

//        //select data
        sql = "SELECT * from throw_test";
        List<Map<String,Object>> result = template.queryForList(sql);
        System.out.println(result.toString());
    }
}
