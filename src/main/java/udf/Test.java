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
    //重载
    public String evaluate(String input){
        String ip_info = "-1";
        try {
            // City类可用于IPDB格式的IPv4免费库，IPv4与IPv6的每周高级版、每日标准版、每日高级版、每日专业版、每日旗舰版
//            URL fileURL=this.getClass().getResource("/resources/mydatavipday3.ipdb");
            InputStream fileURL=this.getClass().getResourceAsStream("/resources/mydatavipday3.ipdb");
            City db = new City(fileURL);
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
        System.out.println(new Test().evaluate("218.22.113.103"));

    }
}