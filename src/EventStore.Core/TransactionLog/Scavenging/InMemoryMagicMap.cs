using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryMagicMap<TStreamId> :
		IMagicForAccumulator<TStreamId>,
		IMagicForCalculator,
		IMagicForExecutor {

		CollisionDetector<TStreamId> _collisionDetector;
		// these are what would be persisted
//qq		private readonly Dictionary<StreamHash, StreamData> _noncollisions;
//qq		private readonly Dictionary<StreamName, StreamData> _collisions;

		// these would just be in mem in proper implementation
		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IIndexReaderForAccumulator<TStreamId> _indexReaderForAccumulator;

		//qq to save us having to look up the stream names repeatedly
		// irl this would be a lru cache.
		//qq		private readonly Dictionary<StreamHash, StreamName> _cacheOf;

		public InMemoryMagicMap(
			ILongHasher<TStreamId> hasher,
			IIndexReaderForAccumulator<TStreamId> indexReaderForAccumulator) {

			var cache = new Dictionary<ulong, TStreamId>();

			_hasher = hasher;
			_indexReaderForAccumulator = indexReaderForAccumulator;

			//qq inject this so that in log v3 we can have a trivial implementation
			_collisionDetector = new CollisionDetector<TStreamId>(HashInUseBefore);

			bool HashInUseBefore(TStreamId recordStream, long recordPosition, out TStreamId candidateCollidee) {
				var hash = _hasher.Hash(recordStream);

				if (cache.TryGetValue(hash, out candidateCollidee))
					return true;

				//qq look in the index for any record with the current hash up to the limit
				// if any exists then grab the stream name for it
				if (_indexReaderForAccumulator.HashInUseBefore(hash, recordPosition, out candidateCollidee)) {
					cache[hash] = candidateCollidee;
					return true;
				}

				cache[hash] = recordStream;
				candidateCollidee = default;
				return false;
			}
		}


		//
		// FOR ACCUMULATOR
		//

		public void Add(TStreamId streamId, long position) {
			_collisionDetector.Add(streamId, position);
		}

		// this will set each time it finds something relevant to scavenge - metadata or tombstone.
		public void Set(TStreamId streamId, StreamData streamData) {
			var streamHash = _hasher.Hash(streamId);

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
