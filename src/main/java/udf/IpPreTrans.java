package udf;


import org.apache.hadoop.util.bloom.Key;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;
import java.util.stream.Collectors;

public class IpPreTrans {
    private static Map<String, ArrayList> stringArrayListMap = new HashMap<>();
    public IpPreTrans() throws IOException
    {
        //创建数组
        String[] str = new String[]{ "40E","001","600","A01","A07","A02","A00","A04","A0B","A05","620","A0C"
                ,"A06","607","A0A","604","A0D","409","A03","804","605","402","404","606"
                ,"602","603","403","400","406","401","408","A09","405","803","610","801"
                ,"A0E","407","C0F","A0F","800","806","40F","601","40C","60C","40A","609"
                ,"40B","608","40D","60F","C0E","802","A08","003"};

        for (String s :str) {
            stringArrayListMap.put(s,new ArrayList<String>());
        }

        //ippre数据入库
        BufferedReader reader = new BufferedReader(new FileReader("D:\\g6hadoop\\src\\main\\resources\\ip_pre.csv"));
        String line = null;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            try
            {
                for (String key:stringArrayListMap.keySet()) {
                    if(line.substring(1,4).equals(key))
                    {
                        stringArrayListMap.get(key).add(line);
                        count++;
                        System.out.println(count + "条ip入库");
                    }

                }
            }
            catch (Exception e)
            {

            }

        }
        reader.close();
    }

    public static void main(String[] args) throws IOException {
        IpPreTrans ipPreTrans = new IpPreTrans();
        long time1 = System.currentTimeMillis();
        //解析100万条ip数据
        BufferedReader reader = new BufferedReader(new FileReader("D:\\g6hadoop\\src\\main\\resources\\ip1000000.csv"));
        String line = null;
        while ((line = reader.readLine()) != null) {
            try
            {
                System.out.println(ipPreTrans.evaluate(line));
            }
            catch (Exception e)
            {

            }

        }
        reader.close();
        System.out.println("花了" + (System.currentTimeMillis() - time1)/60000 + "分钟");
    }

    public String evaluate(String ip)
    {
        String ip_info = "-1";
        if (ip != null) {
            ip = ip.toUpperCase();
            if (ip.matches("[0-9A-F]{1,4}(:[0-9A-F]{1,4}){7}")) {
                //处理ip字段，并且补零
                String ip_pre = String.format
                        ("%4s%4s%4s%4s%4s%4s%4s%4s",
                                ip.split(":")[0]
                                ,ip.split(":")[1]
                                ,ip.split(":")[2]
                                ,ip.split(":")[3]
                                ,ip.split(":")[4]
                                ,ip.split(":")[5]
                                ,ip.split(":")[6]
                                ,ip.split(":")[7]
                        ).replaceAll(" ", "0");
                //在map集合中，寻找对应的ip
                for (String key:stringArrayListMap.keySet()) {
                    if(ip.substring(1,4).equals(key))
                    {
                        List<String> list =  stringArrayListMap.get(key);
                        for(String s:list)
                        {
                            if(ip_pre.contains(s))
                            {
                                return s;
                            }
                        }
                    }

                }

            }

        }
        return ip_info;
    }
}
