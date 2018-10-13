package data.storm.t5;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.trident.TridentWordCount;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.planner.SubtopologyBolt;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Random;

public class TridentCustomTest {
    private static Integer[] amtList = {10,20,30,40,50,60,70,80,90,100};

    private static String[] nameList = {"鼠标","键盘","内存条","硬盘","显示器","机箱","主板","CPU","显卡","电源"};

    private static String[] dateList = {"2018-10-11:12:21:22","2018-10-12:12:21:22",
            "2018-10-13:12:21:22","2018-10-14:12:21:22","2018-10-15:12:21:22","2018-10-16:12:21:22"};

    private static class PrintFilter implements Filter{
        private int partitionCount;

        private int currentPartitionIndex;
        @Override
        public boolean isKeep(TridentTuple tridentTuple) {
            System.out.println("总分区:"+partitionCount+",当前分区:"+currentPartitionIndex+",信息："+tridentTuple);
            return true;
        }

        @Override
        public void prepare(Map map, TridentOperationContext context) {
            partitionCount = context.numPartitions();

            currentPartitionIndex = context.getPartitionIndex();
        }

        @Override
        public void cleanup() {

        }
    }

    private static class OperaFunction implements Function{
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String longDate = tridentTuple.getStringByField("longDate");
            String shortDate = longDate.substring(0,10);
            System.out.println("元数据->日期："+shortDate+",名称:"+tridentTuple.getStringByField("name")+",金额："+tridentTuple.getIntegerByField("amt"));
            tridentCollector.emit(new Values(shortDate));
        }

        @Override
        public void prepare(Map map, TridentOperationContext tridentOperationContext) {

        }

        @Override
        public void cleanup() {

        }
    }
    private static class CustomReducerAggregator implements ReducerAggregator<Long>{
        @Override
        public Long init() {
            return 0L;
        }

        @Override
        public Long reduce(Long l, TridentTuple tridentTuple) {
            long sum = l+tridentTuple.getIntegerByField("amt");
            System.out.println("原:"+l+",相加以后:"+sum);
            return sum;
        }
    }

    private static class CustomCombinerAggregator implements CombinerAggregator<Long>{
        @Override
        public Long init(TridentTuple tridentTuple) {
            long amt = tridentTuple.getIntegerByField("amt").intValue();
            return amt;
        }

        @Override
        public Long combine(Long t1, Long t2) {
            return t1+t2;
        }

        @Override
        public Long zero() {
            return 0L;
        }
    }
    private static class CustomAggregator extends BaseAggregator<CustomAggregator.CountState>{

        static class CountState{
            String batchId;

            long count;

            String date;

            long sum;
        }

        private int partitionCount;

        private int currentPartitionIndex;
        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            this.partitionCount = context.numPartitions();
            this.currentPartitionIndex = context.getPartitionIndex();
            super.prepare(conf, context);
        }

        @Override
        public CountState init(Object batchId, TridentCollector tridentCollector) {
            CountState countState = new CountState();
            countState.batchId = batchId.toString();
            return countState;
        }

        @Override
        public void aggregate(CountState countState, TridentTuple tridentTuple, TridentCollector tridentCollector) {
            countState.count+=1;
            countState.sum+=tridentTuple.getIntegerByField("amt").intValue();
            countState.date = tridentTuple.getStringByField("shortDate");
        }

        @Override
        public void complete(CountState countState, TridentCollector tridentCollector) {
            //System.out.println("日期："+countState.date+",总数:"+countState.sum);
            tridentCollector.emit(new Values(countState.date,countState.sum));
        }
    }

    private static class Split extends BaseFunction{
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String[] keyWords = tridentTuple.getStringByField("args").split(" ");
            for(String keyWord : keyWords){
                tridentCollector.emit(new Values(keyWord));
            }
        }
    }



    public static StormTopology buildTopology(LocalDRPC drpc){
        Random random = new Random();

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("name","amt","longDate"),10,
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)]),
                new Values(nameList[random.nextInt(nameList.length)],amtList[random.nextInt(amtList.length)],dateList[random.nextInt(dateList.length)])

        );
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("test_dataSource",spout);
        //stream.each(new Fields("name", "amt"),new PrintFilter());
        //stream.partitionBy(new Fields("name")).each(new Fields("name","amt"),new PrintFilter()).parallelismHint(10);
        //stream.aggregate(new Fields("amt"),new CustomReducerAggregator(),new Fields("sum")).project(new Fields("sum")).each(new Fields("sum"),new PrintFilter());
        //stream.aggregate(new Fields("amt"),new CustomCombinerAggregator(),new Fields("sum")).each(new Fields("sum"),new PrintFilter());
        //spout的并发度与其他组件相互隔离，互不影响
        //除spout外其他组件如果下行没有设置并行度，则上行设置并行度会影响下行并行度
        //stream = stream.shuffle().parallelismHint(3);//设置3个task下发
        //stream = stream.aggregate(new Fields("amt"),new CustomAggregator(),new Fields("batchId","count")).parallelismHint(5)//设置5个task执行
        //.each(new Fields("batchId","count"),new PrintFilter());//.parallelismHint(10);//设置10个task执行
        TridentState state = stream.each(new Fields("name","amt","longDate"),new OperaFunction(),new Fields("shortDate"))
                .groupBy(new Fields("shortDate","name"))
                .persistentAggregate(new MemoryMapState.Factory(),new Fields("shortDate","amt"),new CustomCombinerAggregator(),new Fields("totalAmt"));


        topology.newDRPCStream("test_drcp",drpc)
                .each(new Fields("args"),new Split(),new Fields("keyWord"))
                .groupBy(new Fields("keyWord"))
                .stateQuery(state,new Fields("keyWord"),new MapGet(),new Fields("shortDate"))
                //.each(new Fields("shortDate"),new FilterNull())
                //.applyAssembly(new FirstN(2,"keyWord",true))
                .project(new Fields("keyWord","shortDate"));
        //SubtopologyBolt
//        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("shortDate"))
//                .groupBy(new Fields("shortDate"))
//                .stateQuery(state, new Fields("shortDate"), new MapGet(), new Fields("totalAmt"))
//                .each(new Fields("totalAmt"), new FilterNull())
//                .project(new Fields("shortDate","totalAmt"))
//                .applyAssembly(new FirstN(10,"totalAmt",true));
                //.project(new Fields("date","name","totalAmt"))
                //.each(new Fields("name","d"),new PrintFilter());
//        topology.newDRPCStream("drpc",drpc).each(new Fields("args"),new Split(),new Fields("searchWord"))
//                .groupBy(new Fields("name"))
//                .stateQuery(state,new Fields("name"),new MapGet(),new Fields("date","name","totalAmt"))
//                .each(new Fields("name"),new FilterNull())
//                .project(new Fields("date","name","totalAmt")).applyAssembly(new FirstN(10,"totalAmt",true))
//                .each(new Fields("date","name","totalAmt"),new PrintFilter());
        //state.newValuesStream().applyAssembly(new FirstN(2,"totalAmt",true)).each(new Fields("shortDate","totalAmt"),new PrintFilter());
                //.aggregate(new Fields("shortDate","amt"),new CustomAggregator(),new Fields("date","totalAmt"))
                //.each(new Fields("date","totalAmt"),new PrintFilter());
        return topology.build();
    }


    public static void main(String[] args) {
        Config config = new Config();
        config.setMaxSpoutPending(20);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("test_topology",config,buildTopology(drpc));
        for(int i=0;i<100;i++){
            System.out.println("DRPCRESULT: " + drpc.execute("test_drcp","2018-10-11 2018-10-12 2018-10-13 2018-10-14 2018-10-15 2018-10-16"));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
