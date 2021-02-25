package rocky.ctrl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.utils.ObjectSerializer;

public class MutationLog {

	public HashMap<Long, MutationRecord> mutationMap;
	public List<MutationRecord> mutationLogSegment;
	public GenericKeyValueStore mutationLogStore;
	
	public class BlockVersion implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1033483351890444051L;
		public int epoch;
		public int count;
	}
	
	public class MutationRecord implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5679557532754097569L;
		public long blockID;
		public BlockVersion version;
		public long timestamp;
		public byte[] blockHash;
	}
	
	public MutationLog(GenericKeyValueStore mlStore) {
		mutationLogStore = mlStore;
	}

	public void addNewMutationRecord(MutationRecord record) {
		if (mutationMap.containsKey(record.blockID)) {
			System.err.println("ASSERT: the block " + record.blockID + " was written already. Haven't you checked dirty bitmap before calling this method?");
			System.exit(1);
		}
		mutationLogSegment.add(record);
		mutationMap.put(record.blockID, record);
	}
	
	public void flush() throws IOException {
		for (MutationRecord rec : mutationLogSegment) {
			byte[] recordBytes = ObjectSerializer.serialize(rec);
			mutationLogStore.put(rec.blockID + "" + rec.version.epoch 
					+ "" + rec.version.epoch, recordBytes);
		}
		mutationLogSegment = new ArrayList<MutationRecord>();
		mutationMap.clear();
	}
}
