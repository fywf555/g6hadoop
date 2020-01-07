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

    static {
        BufferedReader br = new BufferedReader(new InputStreamReader(new Test2().getClass().getResourceAsStream("/ip_pre.csv")));//构造一个BufferedReader类来读取文件
        String s = null;
        while (true) {
            try {
                if (!((s = br.readLine()) != null)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            ipList.add(s);
        }
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //传入ipv6地址，返回ipv6前几个被截取部分（用于匹配码表），否则返回-1
    public String evaluate(String ip) throws IOException {
        String ip_info = "-1";
        if (ip != null) {
            ip = ip.toUpperCase();
            if (ip.matches("[0-9A-F]{1,4}(:[0-9A-F]{1,4}){7}")) {
                StringBuilder ip_pre = new StringBuilder(ip.replaceAll(":", ""));
                System.out.println(ip_pre.toString());
                for (int i = 0; i < ipList.size(); i++) {
                    if (ip_pre.toString().length() >= ipList.get(i).toString().length()) //匹配方法,截取ip的前n位（n的值由传入的ip_pre长度决定），如果相等就返回
                    {
                        if (ip_pre.toString().substring(0, ipList.get(i).toString().length()).equals(ipList.get(i))) {
                            return ipList.get(i).toString();
                        }
                    }
                }
            }
            return ip_info;
        }
        return ip_info;
    }

    public static void main(String[] args) throws IOException {
//        String ip = "2409:0089:1b11:bd50:a8b3:4228:76aa:72db";
        Test2 test2 = new Test2();
        long time1 = System.currentTimeMillis();
        try {
            //先FileReader把文件读出来再bufferReader按行读  reader.readLine(); 没有标题用不着了
            BufferedReader reader = new BufferedReader(new FileReader("D:\\g6hadoop\\src\\main\\resources\\ip1000000.csv"));
            String line = null;
            int index = 0;
            while ((line = reader.readLine()) != null) {
                System.out.println(test2.evaluate(line));
                index++;
            }
            reader.close();
            System.out.println("花了" + (System.currentTimeMillis() - time1)/60000 + "分钟");
        } catch (Exception e) {
            //在命令行打印异常信息在程序中出错的位置及原因。
            e.printStackTrace();
        }
    }

}
