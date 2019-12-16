package udf;

import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
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
import org.apache.hadoop.io.BooleanWritable;

@Description(name = "array_contains", value = "_FUNC_(array, value) - Returns TRUE if the array contains value.", extended = "Example:\n  > SELECT _FUNC_(array(1, 2, 3), 2) FROM src LIMIT 1;\n  true")
public class GenericUDFArrayContains extends GenericUDF
{
    private static final int ARRAY_IDX = 0;
    private static final int VALUE_IDX = 1;
    private static final int ARG_COUNT = 2;
    private static final String FUNC_NAME = "ARRAY_CONTAINS";
    private transient ObjectInspector valueOI;
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;
    private BooleanWritable result;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("The function ARRAY_CONTAINS accepts 2 arguments.");
        }

        if (!(arguments[0].getCategory().equals(ObjectInspector.Category.LIST))) {
            throw new UDFArgumentTypeException(0, "\"array\" expected at function ARRAY_CONTAINS, but \""
                    + arguments[0].getTypeName() + "\" " + "is found");
        }

        this.arrayOI = ((ListObjectInspector) arguments[0]);
        this.arrayElementOI = this.arrayOI.getListElementObjectInspector();

        this.valueOI = arguments[1];

        if (!(ObjectInspectorUtils.compareTypes(this.arrayElementOI, this.valueOI))) {
            throw new UDFArgumentTypeException(1,
                    "\"" + this.arrayElementOI.getTypeName() + "\"" + " expected at function ARRAY_CONTAINS, but "
                            + "\"" + this.valueOI.getTypeName() + "\"" + " is found");
        }

        if (!(ObjectInspectorUtils.compareSupported(this.valueOI))) {
            throw new UDFArgumentException("The function ARRAY_CONTAINS does not support comparison for \""
                    + this.valueOI.getTypeName() + "\"" + " types");
        }

        this.result = new BooleanWritable(false);

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        this.result.set(false);

        Object array = arguments[0].get();
        Object value = arguments[1].get();

        int arrayLength = this.arrayOI.getListLength(array);

        if ((value == null) || (arrayLength <= 0)) {
            return this.result;
        }

        for (int i = 0; i < arrayLength; ++i) {
            Object listElement = this.arrayOI.getListElement(array, i);
            if ((listElement == null)
                    || (ObjectInspectorUtils.compare(value, this.valueOI, listElement, this.arrayElementOI) != 0))
                continue;
            this.result.set(true);
            break;
        }

        return this.result;
    }

    public String getDisplayString(String[] children) {
        assert (children.length == 2);
        return "array_contains(" + children[0] + ", " + children[1] + ")";
    }
}
