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

    public static void main(String[] args) throws Exception {
        Test4 sub = new Test4();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        sub.initialize(new ObjectInspector[]{stringOI});

        for(int i = 0;i<= 10;i++)
        {
            String input = "116.234.222.36";
            Object result = sub.evaluate(new DeferredObject[]{new DeferredJavaObject(input)});
            System.out.println(result.toString());
        }
        sub.close();
    }

}
