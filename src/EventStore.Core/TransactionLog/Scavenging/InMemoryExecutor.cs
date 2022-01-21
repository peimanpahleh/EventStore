namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryExecutor<TStreamId> : IExecutor<TStreamId> {
		public void Execute(IScavengeInstructions<TStreamId> instructions) {
			foreach (var chunkInstructions in instructions.ChunkInstructionss) {
				ExecuteChunk(chunkInstructions);
			}
		}

		private void ExecuteChunk(IReadOnlyChunkScavengeInstructions<TStreamId> chunkInstructions) {
			var threshold = 10 * 1024 * 1024; //qq
			//qq would be nice if we could still remove things from the index even if
			// we didn't remove them from the chunks.
			if (chunkInstructions.BytesToSave < threshold)
				return;

//			monday //qqq
			// 1. open the chunk
			// 2. read through it, keeping and discarding as necessary. probably no additional lookups at this point
			// 3. write the posmap
			// 4. finalise the chunk
			// 5. swap it in to the chunkmanager

		}
	}
}
