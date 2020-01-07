package udf;
import net.ipip.ipdb.City;
import net.ipip.ipdb.IPFormatException;
import net.ipip.ipdb.InvalidDatabaseException;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Arrays;

/**
 * hive的自定义函数
 */
public class Test extends UDF {

    public Test() throws IOException {
    }

    public String evaluate(Text input) throws IOException, IPFormatException {
        String ip_info = "-1";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //读取hdfs文件
        Path path = new Path("hdfs文件路径");
        FSDataInputStream inputStream = fs.open(path);
        City db = new City(inputStream);
        ip_info = Arrays.toString(db.find(input.toString(), "CN"));
        IOUtils.closeQuietly(inputStream);
        return  ip_info;
    }
//'城市'
//'省份代码'
//'省份'
//'国家'
//'ip维度表'
    public static City db;

    static {
        try {
            db = new City("D:\\g6hadoop\\src\\main\\resources\\mydatavipday3.ipdb");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, IPFormatException{

//        String str = "2409:8900:1b11:bd50:a8b3:4228:76aa:72db";
//        System.out.println(ip_info(str));
        FileWriter fileWritter = new FileWriter("D:\\g6hadoop\\src\\main\\resources\\ipv6_trans.csv",true);
        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
        BufferedReader reader = new BufferedReader(new FileReader("D:\\g6hadoop\\src\\main\\resources\\wf_test_tmp_20191230000000__e51d7b66_d04e_48c3_9d21_e07d9c965f63.txt"));
        String line = null;
        int index = 0;
        while ((line = reader.readLine()) != null) {
            try
            {
                bufferWritter.write(ip_info(line) + "\n");
            }
            catch (Exception e)
            {
                bufferWritter.write(line + "," + "" + "," + "" + "," + "" + "," + "" + "\n");
            }

        }
        reader.close();
        bufferWritter.close();
    }

    public static String ip_info(String str) throws IPFormatException, InvalidDatabaseException {
        String s1 = db.find(str,"CN")[2];//国家
        String s2 = db.find(str,"CN")[9];//国家
        String s3 = db.find(str,"CN")[1];//国家
        String s4 = db.find(str,"CN")[0];//国家
        return str + "," + s1 + "," + s2 + "," + s3 + "," + s4;
    }

//    private String readHdfsConfig(String configHdfsBasePath, String name) throws IOException {
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        Path path = new Path(configHdfsBasePath + name);
//        FSDataInputStream inputStream = fs.open(path);
//        String content = IOUtils.toString(inputStream, Charsets.UTF_8);
//        IOUtils.closeQuietly(inputStream);
//        return content;
//    }


}