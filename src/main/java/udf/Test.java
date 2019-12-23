package udf;
import net.ipip.ipdb.City;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;

/**
 * hive的自定义函数
 */
public class Test extends UDF{

    public String evaluate(String input){
        String ip_info = "-1";
        try {
            City db = new City(this.getClass().getResourceAsStream("/mydatavipday3.ipdb"));
            ip_info = Arrays.toString(db.find(input, "CN"));
        }
        catch (Exception e) {
//            e.printStackTrace();

        }
        return ip_info;
    }

    public int evaluate(int a,int b){
        return a+b;//计算两个数之和
    }

    public static void main(String[] args) {

        System.out.println(new Test().evaluate("114.80.166.240"));

    }
}