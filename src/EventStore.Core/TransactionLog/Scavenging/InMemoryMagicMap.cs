using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryMagicMap :
		IMagicForAccumulator,
		IMagicForCalculator,
		IMagicForExecutor {

		// these are what would be persisted
//qq		private readonly Dictionary<StreamHash, StreamData> _noncollisions;
//qq		private readonly Dictionary<StreamName, StreamData> _collisions;

		// these would just be in mem in proper implementation
		private readonly ILongHasher<string> _hasher;
		//qq to save us having to look up the stream names repeatedly
		// irl this would be a lru cache.
//qq		private readonly Dictionary<StreamHash, StreamName> _cacheOf;

		public InMemoryMagicMap(ILongHasher<string> hasher) {
			_hasher = hasher;
		}

		// for accumulator
		// this will set each time it finds something relevant to scavenge - metadata or tombstone.

		public void Set(StreamName streamName, StreamData streamData) {
			var streamHash = _hasher.Hash(streamName.Value);

			throw new System.NotImplementedException();
		}


		public IEnumerable<(StreamHash, StreamData)> RelevantStreamsUncollided => throw new System.NotImplementedException();

		public IEnumerable<(StreamName, StreamData)> RelevantStreamsCollided => throw new System.NotImplementedException();

		public DiscardPoint GetDiscardPoint(StreamName streamName) {
			throw new System.NotImplementedException();
		}

		public DiscardPoint GetDiscardPoint(StreamHash streamHash) {
			throw new System.NotImplementedException();
		}

		public bool IsCollision(StreamHash streamHash) {
			throw new System.NotImplementedException();
		}

		public void Set(StreamName streamName, DiscardPoint dp) {
			throw new System.NotImplementedException();
		}

		public void Add(StreamName streamName) {
			throw new NotImplementedException();
		}
	}
}
//qq note, probably need to complain if the ptable is a 32bit table
//qq maybe we need a collision free core that is just a map
//qq just a thought, lots of the metadatas might be equal, we might be able to store each unique instance once. implementation detail.
