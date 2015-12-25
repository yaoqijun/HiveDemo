package org.yqj.spark.demo;

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
public class TestSparkContent {

    public static void main(String []args){
        System.out.println("test hive content");
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("terminus-spark-context.xml");
//        HiveTemplate hiveTemplate = context.getBean(HiveTemplate.class);
        JdbcTemplate template = context.getBean(JdbcTemplate.class);
//        List<Map<String,Object>> result = template.queryForList("select * from user_test");
//        System.out.println(result.toString());

        String sql = "";
//        sql = "drop table test_create";
//        template.execute(sql);

        //hiveOperations.executeScript(new HiveScript());
//        if(hiveOperations == null){
//            System.out.println("empty");
//            sql = "load data inpath '/user/yaoqijun/part-m-00000' into table test_create";
//            hiveOperations.query(sql);
//        }


//        testCreate(template);
        testSelect(template);
//        testInsert(template);
//        testShowDatabase(template);
//        testDelete(template);
        System.out.println("finish");
    }

    public static void testCreate(JdbcTemplate template){
        String sql = "";
        //test table execute
        sql = " create table stu(id int, name string, age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
        template.execute(sql);
    }

    public static void testShowDatabase(JdbcTemplate template){
        String sql = "";
        //test get database info, get tables info
        sql = "show tables";
        List<Map<String,Object>> result = template.queryForList(sql);
        System.out.println(result.toString());

    }

    public static void testSelect(JdbcTemplate template){
        String sql = "";
        //sql = "show tables";
        //sql = "SELECT sum(sale_qty) AS sale_qty, sum_date as sum_date FROM pig_sales group by sum_date";
//        sql = "select * from test";
        sql = "select NULL, \n" +
                " '2015-10-10 00:00:00',  \n" +
                " s.FarmOID,  \n" +
                " CASE  \n" +
                " WHEN s.EventEName='Removal' then 'SowOutCount'  \n" +
                "      WHEN s.EventEName='PigletsChg' then 'SowPOutCount'  \n" +
                "      WHEN s.EventEName='BoarRemoval' then 'BoarOutCount'  \n" +
                "      ELSE ''  \n" +
                "      END, \n" +
                "      CASE WHEN s.EventEName = 'SowPOutCount' then SUM(s.ChgCount)  \n" +
                "      ELSE COUNT(s.OID) END AS OutCount, \n" +
                "      SUM(s.EWeight) AS OutWeight, \n" +
                "      NULL \n" +
                "FROM  \n" +
                "( \n" +
                "  select OID, EWeight, ChgType, FarmOID, EventEName, ChgCount from  view_EventListSow  \n" +
                "  where EventEName in ('Removal', 'PigletsChg')  \n" +
                "        AND EventDate >'2015-10-10 00:00:00'  \n" +
                "        AND EventDate<'2015-10-10 00:00:00'  \n" +
                "  union all  \n" +
                "  select OID, EWeight, ChgType, FarmOID, EventEName, NULL as ChgCount from view_EventListBoar  \n" +
                "  WHERE EventEName = 'BoarRemoval'  \n" +
                "        AND EventDate >'2015-10-10 00:00:00'  \n" +
                "        AND EventDate<'2015-10-10 00:00:00'  \n" +
                ") as s  \n" +
                "LEFT JOIN TB_FieldValue AS c ON s.ChgType = c.ColID  \n" +
                "WHERE  \n" +
                "    c.TypeID = '变动类型'  \n" +
                "    AND c.Remark = '计入出栏猪'  \n" +
                "group by s.FarmOID, s.EventEName";
        List<Map<String,Object>> result2 = template.queryForList(sql);
        result2.forEach(v -> {
            System.out.println(v.toString());
        });
    }

    public static void testInsert(JdbcTemplate template){
        String sql = "";
//        sql = "insert into table parana_addresses_again VALUES(1,1,'test',1,1)";
//        sql = "INSERT INTO TABLE parana_addresses_again SELECT * from parana_addresses";
        sql = "insert into table test select * from test";
        template.execute(sql);
        System.out.println(template.queryForList("SELECT * from stu").size());
    }

    public static void testDelete(JdbcTemplate template){
        String sql = "";
        sql = "delete from stu where name = '姚'";
        template.execute(sql);
    }
}
