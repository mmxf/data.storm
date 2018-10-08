package data.storm.t2;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;

public class Bolt extends BaseWindowedBolt {

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tupleList = tupleWindow.get();
        int totalAmt = 0;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < tupleList.size(); i++) {
            Tuple e = tupleList.get(i);
            totalAmt += Integer.parseInt(e.getStringByField("amt"));
        }
        System.out.println("总额："+totalAmt+",数量:"+tupleList.size());
    }
}
