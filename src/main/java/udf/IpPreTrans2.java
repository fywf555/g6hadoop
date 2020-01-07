package udf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

//尝试使用迭代算法
public class IpPreTrans2 {
    private static List<Map<String,ArrayList>> mapList;
    private static Map<String, ArrayList> stringArrayListMap = new HashMap<>();

    IpPreTrans2() throws IOException {
        //创建数组
        String[] str = new String[]{"0","1","2","3","4","5","6","7","8","9","A","B","C" ,"D" ,"E" ,"F" };
        //迭代入库
        //ippre数据入库
        BufferedReader reader = new BufferedReader(new FileReader("D:\\g6hadoop\\src\\main\\resources\\ip_pre.csv"));
        String line = null;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            try
            {
//                for (String key:stringArrayListMap.keySet()) {
//                    if(line.substring(1,4).equals(key))
//                    {
//                        stringArrayListMap.get(key).add(line);
//                        count++;
//                        System.out.println(count + "条ip入库");
//                    }
//
//                }
            }
            catch (Exception e)
            {

            }

        }
        reader.close();
    }

}
