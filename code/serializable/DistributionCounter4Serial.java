package serializable;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import abstraction.Transaction;
import accessdistribution.*;
import config.Configurations;

public class DistributionCounter4Serial {
	// 针对所有事务中所有参数 统计好的访问分布都存放在该数据结构中。事务名称 -> 参数标示符 -> 数据分布信息列表
	// 参数标示符格式：operationId_index index从0开始
	// 注意在目前的机制下（同一个参数的数据访问分布肯定由同一个线程按序分析得到），下面的数据结构不用Vector，可直接用ArrayList
	public Map<String, Map<String, Vector<DataAccessDistribution>>> txName2ParaId2DistributionList = null;
	//added by qly
	public transient List<Transaction> transactions = null;
	//------
	// 事务名称 -> 各个时间窗口的吞吐信息
	public Map<String, Vector<Throughput>> txName2ThroughputList = null;

	// 利用采样的数据 统计 得到全负载周期数据访问分布
	public Map<String, Map<String, DataAccessDistribution>> txName2ParaId2FullLifeCycleDistribution = null;

	public DistributionCounter4Serial(List<Transaction> transactions) {
		super();
		this.transactions = transactions;
		this.txName2ParaId2DistributionList = DistributionCounter.getTxName2ParaId2DistributionList();
		//这里要改的~~
//		this.txName2ParaId2DistributionList = new HashMap<String, Map<String, Vector<ContinuousParaDistribution<Number>>>>();
//		Map<String, Map<String, Vector<DataAccessDistribution>>> tmpmap1 = DistributionCounter
//				.getTxName2ParaId2DistributionList();
//		for (Entry<String, Map<String, Vector<DataAccessDistribution>>> entry1 : tmpmap1.entrySet()) {
//			Map<String, Vector<ContinuousParaDistribution<Number>>> mymap = new HashMap<String, Vector<ContinuousParaDistribution<Number>>>();
//			for (Entry<String, Vector<DataAccessDistribution>> entry2 : entry1.getValue().entrySet()) {
//				Vector<ContinuousParaDistribution<Number>> myvector = new Vector<ContinuousParaDistribution<Number>>();
//				for (DataAccessDistribution da : entry2.getValue()) {
//					myvector.add((ContinuousParaDistribution<Number>) da);
//				}
//				mymap.put(entry2.getKey(), myvector);
//			}
//			this.txName2ParaId2DistributionList.put(entry1.getKey(), mymap);
//		}

		this.txName2ThroughputList = DistributionCounter.getOriginTxName2ThroughputList();

		this.txName2ParaId2FullLifeCycleDistribution = DistributionCounter.getTxName2ParaId2FullLifeCycleDistribution();
		if(Configurations.getFakeColumnRate() != 0){
			FakeAccessDistribution fakeAccessDistribution = new FakeAccessDistribution(transactions,txName2ParaId2DistributionList,txName2ParaId2FullLifeCycleDistribution);
			fakeAccessDistribution.addTxname2ParaIdDistributionList();
			this.txName2ParaId2DistributionList = fakeAccessDistribution.getTxName2ParaId2DistributionList();
			this.txName2ParaId2FullLifeCycleDistribution = fakeAccessDistribution.getTxName2ParaId2FullLifeCycleDistribution();
		}

//		this.txName2ParaId2FullLifeCycleDistribution = new HashMap<String, Map<String, ContinuousParaDistribution<Number>>>();
//		Map<String, Map<String, DataAccessDistribution>> tmpmap2 = DistributionCounter
//				.getTxName2ParaId2FullLifeCycleDistribution();
//		for (Entry<String, Map<String, DataAccessDistribution>> entry1 : tmpmap2.entrySet()) {
//			Map<String, ContinuousParaDistribution<Number>> mymap = new HashMap<String, ContinuousParaDistribution<Number>>();
//			for (Entry<String, DataAccessDistribution> entry2 : entry1.getValue().entrySet()) {
//				mymap.put(entry2.getKey(), (ContinuousParaDistribution<Number>) entry2.getValue());
//			}
//			this.txName2ParaId2FullLifeCycleDistribution.put(entry1.getKey(), mymap);
//		}

	}

}
