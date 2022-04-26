package accessdistribution;

public class Throughput implements Comparable<Throughput> {

	String txName;
	long time;
	int throughput; // 这个吞吐量是针对一个时间窗口大小的

	public Throughput(String txName, long time, int throughput) {
		super();
		this.txName = txName;
		this.time = time;
		this.throughput = throughput;
	}

	@Override
	public int compareTo(Throughput o) {
		if (this.time < o.time) {
			return -1;
		} else if (this.time > o.time) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return "Throughput [txName=" + txName + ", time=" + time + ", throughput=" + throughput + "]";
	}
}