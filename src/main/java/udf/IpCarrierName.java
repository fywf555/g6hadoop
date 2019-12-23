package udf;

import net.ipip.ipdb.City;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

@Description(name = "array_contains", value = "_FUNC_(array, value) - Returns TRUE if the array contains value.", extended = "Example:\n  > SELECT _FUNC_(array(1, 2, 3), 2) FROM src LIMIT 1;\n  true")
public class IpCarrierName extends GenericUDF {
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

        String ip_info = "";
        try {
            ip_info = DB.find(cgi.toString(), "CN")[4];
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
}
