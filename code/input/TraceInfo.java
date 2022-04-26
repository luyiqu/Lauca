package input;

import java.util.List;

/**
 * 负载轨迹信息，Tidb日志分析时使用。一个操作对应一个TraceInfo
 * 
 * @author Shuyan Zhang
 *
 */
public class TraceInfo {
	public int operationID;
	public List<String> parameters = null;
	/** 一个SQL的返回结果集，可能有多行；如果没有就是null */
	public List<List<String>> results = null;
	public long operationTS;
	/** 是否批处理 */
	boolean isBatched = false;
	@Override
	public String toString() {
		return "" + operationID + " " + parameters + " " + results + "\n";
	}
}
