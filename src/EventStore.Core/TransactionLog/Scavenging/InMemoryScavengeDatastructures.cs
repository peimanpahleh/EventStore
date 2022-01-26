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

	//qq this may end up being a wrapper around the magic map and not a memory specific implementation
	// at all.
	//qq instructions are not necessarily the right name for this now.
	public class InMemoryScavengeInstructions<TStreamId> : IScavengeInstructions<TStreamId> {
		public InMemoryScavengeInstructions() {
		}

		public IEnumerable<IReadOnlyChunkScavengeInstructions<TStreamId>> ChunkInstructionss => throw new System.NotImplementedException();

		public bool TryGetDiscardPoint(TStreamId streamId, out DiscardPoint discardPoint) {
			throw new System.NotImplementedException();
		}
	}

	public class InMemoryChunkScavengeInstructions<TStreamId> : IChunkScavengeInstructions<TStreamId> {
		public int ChunkNumber => throw new System.NotImplementedException();

		public IDictionary<TStreamId, long> EarliestEventsToKeep {
			get => throw new System.NotImplementedException();
			set => throw new System.NotImplementedException();
		}

		public int NumRecordsToDiscard { get; private set; }

		public void Discard(TStreamId streamId, long position, int sizeInbytes) {
			throw new System.NotImplementedException();
		}
	}
}
