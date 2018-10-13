/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package data.storm.t4;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.spout.TridentSpoutCoordinator;
import org.apache.storm.trident.spout.TridentSpoutExecutor;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.topology.MasterBatchCoordinator;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class TridentWordCount {
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
            new Values("the cow jumped over the moon"),
            new Values("the man went to the store and bought some candy"),
            new Values("four score and seven years ago"),
            new Values("how many apples can you eat"),
            new Values("to be or not to be the person")
    );
    spout.setCycle(true);

    //MasterBatchCoordinator  spout

    //TridentSpoutCoordinator bolt

    //TridentSpoutExecutor bolt



    TridentTopology topology = new TridentTopology();
    TridentState wordCounts = topology.newStream("spout1", spout)
            .parallelismHint(16)
            .each(new Fields("sentence"),new Split(), new Fields("word"))
            .groupBy(new Fields("word"))
            .persistentAggregate(new MemoryMapState.Factory(),new Count(), new Fields("count"))
            .parallelismHint(16);
    wordCounts.newValuesStream().each(new Fields("word","count"), new BaseFilter() {
      @Override
      public boolean isKeep(TridentTuple tridentTuple) {
        System.out.println("custom print count:"+tridentTuple.get(0)+"|"+tridentTuple.get(1));
        return true;
      }
    });
    topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word"))
            .groupBy(new Fields("word"))
            .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
            .each(new Fields("count"), new FilterNull())
            .project(new Fields("word", "count"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      //LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(null));
//      for (int i = 0; i < 100; i++) {
//        System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
//        Thread.sleep(2000);
//      }
    }else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
    }
  }
}
