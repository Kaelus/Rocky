package rocky.ctrl;

import java.io.IOException;

import rocky.ctrl.MutationLog.BlockVersion;
import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.utils.ObjectSerializer;

public class VersionMap {

	public GenericKeyValueStore versionMapStore;
	
	public VersionMap(GenericKeyValueStore vmStore) {
		versionMapStore = vmStore;
	}
	
	public void addVersionMapping(long blockID, BlockVersion version) throws IOException {
		versionMapStore.put("" + blockID, ObjectSerializer.serialize(version));
	}
	
	public BlockVersion getVersionMapping(long blockID) throws IOException, ClassNotFoundException {
		byte[] versionBytes = versionMapStore.get("" + blockID);
		return (BlockVersion) ObjectSerializer.deserialize(versionBytes);
	}
}
