package udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

public class UdafTest extends UDAF
{
    public static class midResult
    {
        public long numCount;
        public double multSum;
    }

    public static class GMEvaluator implements UDAFEvaluator
    {
        midResult midr;
        public GMEvaluator()
        {
            super();
            midr = new midResult();
            init();
        }
        public void init()//对中间结果实现初始化
        {
            midr.multSum = 1;
            midr.numCount = 0;
        }
        public boolean iterate(IntWritable a)//接受传入的参数，并进行内部的轮转
        {
            if(a!=null)
            {
                midr.multSum*=a.get();
                midr.numCount++;
            }
            return true;
        }
        public midResult terminatePartial()//负责返回iterate函数轮转后的数据
        {
            return midr.numCount==0?null:midr;
        }
        public boolean merge(midResult b)//接受terminatePartial的返回结果，合并接受的中间值
        {
            if(b!=null)
            {
                midr.numCount*=b.numCount;
                midr.multSum+=b.multSum;
            }
            return true;
        }
        public Double terminate()//返回最终的结果
        {
            return midr.numCount==0?null:Math.pow(midr.multSum,1.0/midr.numCount);
        }
    }
}