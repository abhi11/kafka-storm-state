/**
 * Transactional Spout with kafka acting as the source.
 * Takes data from a single kafka topic partition and 
 * emits it as a batch of tuple.
 */

package storm.ubiquitous.spouts;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.HashMap;
import java.util.Map;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class KafkaSpoutTransaction extends BasePartitionedTransactionalSpout<TransactionMetadata>{

    @Override
	public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf,TopologyContext context){
	return new KafkaPartitionedCoordinator();
    }
    @Override
	public IPartitionedTransactionalSpout.Emitter<TransactionMetadata> getEmitter(Map conf,TopologyContext context){
	return new KafkaPartitionedEmitter();
    }

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
	declarer.declare(new Fields("txid","data"));
    }
    public static class KafkaPartitionedCoordinator implements IPartitionedTransactionalSpout.Coordinator{

	@Override
	public int numPartitions(){
	    return 2;
	}
	@Override
	public boolean isReady(){
	    return true;
	}
	@Override
	public void close(){
	}
    }
    public static class KafkaPartitionedEmitter implements IPartitionedTransactionalSpout.Emitter<TransactionMetadata>{
	@Override
	    public TransactionMetadata emitPartitionBatchNew(TransactionAttempt tx,BatchOutputCollector collector,int partition,TransactionMetadata lastPartitionMeta){
	    //use kafka and partition no. to fetch the data. 
	    TransactionMetadata metadata = new TransactionMetadata(0,0L);
	    KafkaConsumer consumer = new KafkaConsumer(partition);
	    try{
		ByteBufferMessageSet messageSet =  consumer.fetchdata();
		//create another TransactionMetadata obj and return it to be stored by zookeeper
		//and to be used later for replay.
		System.out.println("Inside emitpartitionbatchnew. Fetching partition "+partition);
		int flag = 0;
		for(MessageAndOffset messageAndOffset: messageSet) {
		    if(flag == 0){
			long offset = messageAndOffset.offset();
			metadata = new TransactionMetadata(partition,offset);
			flag=1;
		    }
		    ByteBuffer payload = messageAndOffset.message().payload();
		    byte[] bytes = new byte[payload.limit()];
		    payload.get(bytes);
		    collector.emit(new Values(tx, new String(bytes,"UTF-8")));
		    //return new String(bytes, "UTF-8");
		}
	    }
	    catch(Exception e){
	    }
	    return metadata;	
	}
	@Override
	    public void emitPartitionBatch(TransactionAttempt tx,BatchOutputCollector collector,int partition,TransactionMetadata partitionMeta){
	    //Replays the complete partition use kafka and partition no to fetch the data 
	    //use kafka and partition no. to fetch the data.
	    int metapartition = partitionMeta.partition;
	    KafkaConsumer consumer = new KafkaConsumer(metapartition);
	    try{
		ByteBufferMessageSet messageSet =  consumer.fetchdata();
		//create another TransactionMEtadata obj and return it to be stored by zookeeper
		//and to be used later for replay.
		System.out.println("Inside emitpartitionbatch. Fetching partition "+metapartition);
		for(MessageAndOffset messageAndOffset: messageSet) {
		    ByteBuffer payload = messageAndOffset.message().payload();
		    byte[] bytes = new byte[payload.limit()];
		    payload.get(bytes);
		    collector.emit(new Values(tx, new String(bytes,"UTF-8")));
		    //return new String(bytes, "UTF-8");
		} 
	    }
	    catch(Exception e){
	    }
	}
	@Override
	    public void close(){
	}
    }
}
