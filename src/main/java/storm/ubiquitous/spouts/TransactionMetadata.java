
package storm.ubiquitous.spouts;

public class TransactionMetadata implements Serializable{
    private static final long serialVersionUID = 1L;
    public int partition;
    public long offset;
    public TransactionMetadata(int partition,long offset){
	this.partition = partition;
	this.offset = offset;
    }
}
