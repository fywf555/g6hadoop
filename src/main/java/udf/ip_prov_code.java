package udf;

import net.ipip.ipdb.City;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.InputStream;
import java.util.Arrays;

public class ip_prov_code extends UDF
{
    //重载
    public String evaluate(String input){
        String ip_info = "-1";
        try {
            // City类可用于IPDB格式的IPv4免费库，IPv4与IPv6的每周高级版、每日标准版、每日高级版、每日专业版、每日旗舰版
            InputStream in=this.getClass().getResourceAsStream("/mydatavipday3.ipdb");
            City db = new City(in);
            ip_info = Arrays.toString(db.find(input, "CN"));
            ip_info = ip_info.split(",")[9];
        }
        catch (Exception e) {
//            e.printStackTrace();

        }
        return ip_info;
    }
}
