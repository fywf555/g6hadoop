package udf;
import net.ipip.ipdb.City;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.io.IOException;
import java.util.Arrays;

/**
 * 调整小区的格式
 * 460-00-394487-1 --> 394487_1
 */

public class Test4 extends GenericUDF {
    private transient StringObjectInspector allCgi;
    private static City DB;
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        ObjectInspector a = arguments[0];
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The operator 'SubstrCgi' accepts one arguments.");
        }
        try
        {
            DB = new City(this.getClass().getResourceAsStream("/mydatavipday3.ipdb"));
        } catch (IOException e)
        {
        }
        this.allCgi = (StringObjectInspector) a;
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String cgi = allCgi.getPrimitiveJavaObject(arguments[0].get());
        if(null == cgi) {
            return null;
        }

        String ip_info = "-1";
        try {
            ip_info = Arrays.toString(DB.find(cgi.toString(), "CN"));
        }
        catch (Exception e) {
        }
//        StringBuffer sb = new StringBuffer().append(ip_info);
        return ip_info;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: SubstrCgi(String cgi)";
    }

//    public static void main(String[] args) throws Exception {
//        Test4 sub = new Test4();
//        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//        sub.initialize(new ObjectInspector[]{stringOI});
//
//        for(int i = 0;i<= 10;i++)
//        {
//            String input = "116.234.222.36";
//            Object result = sub.evaluate(new DeferredObject[]{new DeferredJavaObject(input)});
//            System.out.println(result.toString());
//        }
//        sub.close();
//    }

    public static void main(String[] args) {
        String ip = "2409:89:1b11:bd50:a8b3:4228:76aa:72db";
        ip = ip.toUpperCase();
        ip = String.format
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
//        StringBuilder ip_pre = new StringBuilder(format.replaceAll(" ", "0"));
        System.out.println(ip);


//        if (ip.matches("[0-9A-F]{1,4}(:[0-9A-F]{1,4}){7}")) {
//            String[] split = ip.split(":");
//            String str = "";
//            for (int i = 0; i < split.length; i++) {
//                String s = split[i];
//                System.out.println(String.format("%04s" ,11));
//                str += String.format("%04d", s);
//                str += s;
//            }
//            ip = str;
//            System.out.println(ip);
//        }
    }
}
