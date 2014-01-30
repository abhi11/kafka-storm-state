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

public class KafkaSpoutTransaction extends BasePartitionedTransactionalSpout<TransactionMetadata>{

    @Override
	public IPartitionedTransactionalSpout.Coordinator<TransactionMetadata> getCoordinator(Map conf,TopologyContext context){
	return new KafkaPartitionedCoordinator();
    }
    @Override
	public IPartitionedTransactionalSpout.Emitter<TransactionMetadata> getEmitter(Map conf,TopologyContext context){
	return new KafkaPartitionedEmitter();
    }

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
	declare.declare(new Fields("txid","data"));
    }
    public static KafkaPartitionedCoordinator implements Coordinator{

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
    public static KafkaPartitionedEmitter implements Emitter{
	public TransactionMetadata emitPartitionBatchNew(TransactionAttempt tx,BatchOutputCollector collector,int partition,TransactionMetadata lastPartitionMeta){
	    //use kafka and partition no. to fetch the data. 
	    KafkaConsumer consumer = new KafkaConsumer(partition);
	    ByteBufferMessageSet messageSet =  consumer.fetchdata();
	    //create another TransactionMEtadata obj and return it to be stored by zookeeper
	    //and to be used later for replay.
	    System.out.println("Inside emitpartitionbatchnew. Fetching partition "+partition);
	    int flag = 0;
	    for(MessageAndOffset messageAndOffset: messageSet) {
		if(flag == 0){
		    long offset = messageAndOffset.offset();
		    TransactionMetadata metadata = new TransactionMetadata(partition,offset);
		    flag=1;
		}
		ByteBuffer payload = messageAndOffset.message().payload();
		byte[] bytes = new byte[payload.limit()];
		payload.get(bytes);
		collector.emit(tx, new String(bytes,"UTF-8"));
		//return new String(bytes, "UTF-8");
	    }
	    return metadata;	
	}
	public void emitPartitionBatch(TransactionAttempt tx,BatchOutputCollector collector,int partition,TransactionMetadata partitionMeta){
	    //Replays the complete partition use kafka and partition no to fetch the data 
	    //use kafka and partition no. to fetch the data.
	    int metapartition = partitionMeta.partition;
	    KafkaConsumer consumer = new KafkaConsumer(metapartition);
	    ByteBufferMessageSet messageSet =  consumer.fetchdata();
	    //create another TransactionMEtadata obj and return it to be stored by zookeeper
	    //and to be used later for replay.
	    System.out.println("Inside emitpartitionbatch. Fetching partition "+metapartition);
	    for(MessageAndOffset messageAndOffset: messageSet) {
		ByteBuffer payload = messageAndOffset.message().payload();
		byte[] bytes = new byte[payload.limit()];
		payload.get(bytes);
		collector.emit(tx, new String(bytes,"UTF-8"));
		//return new String(bytes, "UTF-8");
	    } 
	}
	public void close(){
	}
    }
}
