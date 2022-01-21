using System.Collections.Generic;
using System.Linq;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryScavengeState<TStreamId> : IScavengeState<TStreamId> {
		private readonly IDictionary<TStreamId, StreamData> _dict;

		public InMemoryScavengeState(IDictionary<TStreamId, StreamData> dict) {
			_dict = dict;
		}

		public IEnumerable<(TStreamId, StreamData)> RelevantStreams =>
			_dict.Select(kvp => (kvp.Key, kvp.Value));
	}

	public class InMemoryScavengeInstructions<TStreamId> : IScavengeInstructions<TStreamId> {
		public InMemoryScavengeInstructions() {
		}

		public IEnumerable<IReadOnlyChunkScavengeInstructions<TStreamId>> ChunkInstructionss =>
			throw new System.NotImplementedException();

		public IIndexScavengeInstructions IndexInstructions =>
			throw new System.NotImplementedException();
	}

	public class InMemoryChunkScavengeInstructions<TStreamId> : IChunkScavengeInstructions<TStreamId> {
		public int ChunkNumber => throw new System.NotImplementedException();

		public IDictionary<TStreamId, long> EarliestEventsToKeep {
			get => throw new System.NotImplementedException();
			set => throw new System.NotImplementedException();
		}

		public int BytesToSave { get; private set; }

		public void Discard(TStreamId streamId, long position, int sizeInbytes) {
			throw new System.NotImplementedException();
		}
	}
}
