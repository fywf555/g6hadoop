package udf;
import net.ipip.ipdb.City;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
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
public class Test2 extends GenericUDF
{
    private transient ObjectInspector valueOI;
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;
    private transient StringObjectInspector stringElementOI;
    private BooleanWritable result;
    private static City DB;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("The function ARRAY_CONTAINS accepts 1 arguments.");
        }
        //初始化本地变量
        try
        {
            DB = new City(this.getClass().getResourceAsStream("/mydatavipday3.ipdb"));
        } catch (IOException e)
        {
            e.printStackTrace();
        }

//        this.arrayOI = ((ListObjectInspector) arguments[0]);
//        this.arrayElementOI = this.arrayOI.getListElementObjectInspector();
        this.stringElementOI = ((StringObjectInspector) arguments[0]);
        this.valueOI = arguments[0];

        if (!(ObjectInspectorUtils.compareTypes(this.arrayElementOI, this.valueOI))) {
            throw new UDFArgumentTypeException(1,
                    "\"" + this.arrayElementOI.getTypeName() + "\"" + " expected at function, but "
                            + "\"" + this.valueOI.getTypeName() + "\"" + " is found");
        }

        if (!(ObjectInspectorUtils.compareSupported(this.valueOI))) {
            throw new UDFArgumentException("The function does not support comparison for \""
                    + this.valueOI.getTypeName() + "\"" + " types");
        }

        this.result = new BooleanWritable(false);
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    public String evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        this.result.set(false);
        Object array = arguments[0].get();
        String ip_info = "-1";
        try {
            ip_info = Arrays.toString(DB.find(array.toString(), "CN"));
            ip_info = ip_info.split(",")[9];
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return ip_info;
    }

    public String getDisplayString(String[] children) {
        return "test";
    }
}

