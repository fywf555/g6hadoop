package udf;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * hive的自定义函数
 */
public class Test2 extends UDF {
    public static List ipList = new ArrayList<String>();
    static
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(new Test2().getClass().getResourceAsStream("/ip_pre.csv")));//构造一个BufferedReader类来读取文件
        String s = null;
        while(true){
            try {
                if (!((s = br.readLine())!=null)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            ipList.add(s);
        }
    }

    //传入ipv6地址，返回ipv6前几个被截取部分（用于匹配码表），否则返回-1
    public String evaluate(String ip) throws IOException {
        String ip_info = "-1";
        BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/ip_pre.csv")));//构造一个BufferedReader类来读取文件
        if(ip != null)
        {
            ip = ip.toUpperCase();
            if(ip.matches("[0-9A-F]{1,4}(:[0-9A-F]{1,4}){7}"))
            {
                String[] split = ip.split(":");
                String str = "";
                for (int i = 0; i < split.length; i++) {
                    String s = split[i];
                    str += String.format("%04d", s);
                }
                ip = str;
                StringBuilder ip_pre = new StringBuilder(ip.replaceAll(":", ""));
                System.out.println(ip_pre.toString());
                String s = null;
                while((s = br.readLine())!=null){
                    //使用readLine方法，一次读一行
                    if( ip_pre.toString().length() >= s.length()) //匹配方法,截取ip的前n位（n的值由传入的ip_pre长度决定），如果相等就返回
                    {
                        if(ip_pre.toString().substring(0,s.length()).equals(s))
                        {
                            return s;
                        }
                    }
                }
            }
        }
        br.close();
        return ip_info;
    }

    public static void main(String[] args) throws IOException{
//        String str = "2409:8900:1b11:bd50:a8b3:4228:76aa:72db";
        String str = "240e:00d8:0111:499d:e8b0:76a5:2e04:a518";
        System.out.println(new Test2().evaluate(str));


//        Test2 test2 = new Test2();
//        try {
//            //先FileReader把文件读出来再bufferReader按行读  reader.readLine(); 没有标题用不着了
//            BufferedReader reader = new BufferedReader(new FileReader("D:\\g6hadoop\\src\\main\\resources\\ip1000000.csv"));
//            String line = null;
//            int index = 0;
//            while ((line = reader.readLine()) != null) {
//                System.out.println(test2.evaluate(line));
//            }
//            reader.close();
//        } catch (Exception e) {
//            //在命令行打印异常信息在程序中出错的位置及原因。
//            e.printStackTrace();
//        }

    }

}
