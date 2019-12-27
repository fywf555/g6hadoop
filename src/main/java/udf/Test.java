package udf;
import net.ipip.ipdb.City;
import net.ipip.ipdb.IPFormatException;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * hive的自定义函数
 */
public class Test extends UDF {

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

    public static void main(String[] args) throws IOException, IPFormatException{

        City db = new City("D:\\g6hadoop\\src\\main\\resources\\mydatavipday3.ipdb");
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