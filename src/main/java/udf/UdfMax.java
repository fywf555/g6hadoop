package udf;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


/***
 * 尝试udaf和udtf混用，udaf处理的中间结果导出来，由udtf进行输出
 * 重写terminate()函数，terminate底层应该为reduce函数，如果能重写就能多行输出。
 *
 */

public class UdfMax {


    //udaf 部分
    public static class midResult
    {
        public String ip_info;
    }
    //udaf 部分


    public static class GMEvaluator implements UDAFEvaluator
    {
        UdfMax.midResult midr;
        public GMEvaluator()
        {
            super();
            midr = new UdfMax.midResult();
            init();
        }
        public void init()//对中间结果实现初始化
        {
            midr.ip_info = "-1";
        }
        public boolean iterate(Text a)//接受传入的参数，并进行内部的轮转
        {
            if(a!=null)
            {
                midr.ip_info = midr.ip_info + "," + a.toString();
            }
            return true;
        }
        public UdfMax.midResult terminatePartial()//负责返回iterate函数轮转后的数据
        {
            return midr;
        }

//        public boolean merge(UdfMax.midResult b)//接受terminatePartial的返回结果，合并接受的中间值
//        {
//            if(b!=null)
//            {
//                midr.numCount*=b.numCount;
//                midr.multSum+=b.multSum;
//            }
//            return true;
//        }

        public String terminate()//返回最终的结果
        {
            //在这里面写udtf
            return null;
        }
    }

    public static class udtfResult extends GenericUDTF
    {
        //udtf 部分
        @Override
        public void process(Object[] objects) throws HiveException {

        }

        @Override
        public void close() throws HiveException {

        }
    }


}
