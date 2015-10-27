package org.yqj.hive.demo;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HadoopFileUtil {
    public static void main(String[] args){

        System.out.println("hadoop file util");

        try{
            AbstractApplicationContext context = new ClassPathXmlApplicationContext("hadoop-context.xml");
            Resource resource = context.getResource("/user/yaoqijun/teacher/part-m-00000");
            System.out.println(resource.getFile().length());
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("hadoop file error");
        }

    }
}
