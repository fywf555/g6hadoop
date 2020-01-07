package udf;


import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 以空格拆分，判断个数
 * 1室 整租20㎡ 共2层 [3个参数]
 * 4室2厅2卫 次卧153㎡ 向北 低层/共6层 中等装修 普通住宅 [6个参数]
 * 将以上两周情况拆分为7个字段,没有置空，依次为：【户型、面积、朝向、层数、装修状况、住宅类型、租赁方式】。
 *
 * @author leen 2017-02-16
 */

public class UdtfTest extends GenericUDTF {

    /**
     * 重写initialize方法，指定返回字段名称
     * 在hive-0.13.0之后，已经弃用
     *
     * @param arg0
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        //判断传入参数个数
        if (arg0.length != 2) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }
        //判断出入参数类型
        if (arg0[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("hou_model");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("size");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("direction");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("floor");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("fitm_status");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("house_class");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("rent");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * @param arg0
     * @throws HiveException
     */
    @Override
    public void process(Object[] arg0) throws HiveException {
        String input = arg0[0].toString();//输入的数据
        String split = arg0[1].toString();//输入的空格
        String[] words = input.split(split);

        String word2 = words[1];
        Pattern p = Pattern.compile("([\\u4e00-\\u9fa5]+)(\\d+)");
        Matcher m = p.matcher(word2);
        if (m.find()) {
            String[] words6 = new String[7];
            if (words.length == 6) {
                words6[0] = words[0];
                words6[1] = m.group(2) + "m²";
                words6[2] = words[2];
                words6[3] = words[3];
                words6[4] = words[4];
                words6[5] = words[5];
                words6[6] = m.group(1);
                forward(words6);
            }

            String[] words3 = new String[7];
            if (words.length == 3) {
                words3[0] = words[0];
                words3[1] = m.group(2) + "m²";
                words3[2] = "";
                words3[3] = words[2];
                words3[4] = "";
                words3[5] = "";
                words3[6] = m.group(1);
                forward(words3);
            }
        } else {
            String[] words0 = {"", "", "", "", "", "", ""};
            forward(words0);
        }
    }

    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub
    }

    public static void main(String[] args)
    {
    }
}

