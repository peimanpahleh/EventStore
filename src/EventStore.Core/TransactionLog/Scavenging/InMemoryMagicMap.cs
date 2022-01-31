using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {

	public class InMemoryMagicMap<TStreamId> :
		IMagicForAccumulator<TStreamId>,
		IMagicForCalculator,
		IMagicForExecutor {

		private readonly CollisionDetector<TStreamId> _collisionDetector;

		// these are what would be persisted
		private readonly InMemoryCollisionResolver<TStreamId, StreamHash, StreamData> _metadatas;
		private readonly InMemoryCollisionResolver<TStreamId, StreamHash, DiscardPoint> _scavengeableStreams;

		// these would just be in mem even in proper implementation
		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IIndexReaderForAccumulator<TStreamId> _indexReaderForAccumulator;

		public InMemoryMagicMap(
			ILongHasher<TStreamId> hasher,
			IIndexReaderForAccumulator<TStreamId> indexReaderForAccumulator) {

			_metadatas = new();
			_scavengeableStreams = new();

			//qq to save us having to look up the stream names repeatedly
			// irl this would be a lru cache.
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

		public void NotifyForCollisions(TStreamId streamId, long position) {
			_collisionDetector.Add(streamId, position);
		}

		public void NotifyForScavengeableStreams(TStreamId streamId) {
			// register this stream as a scavengeable stream
			//qq is it ok that this is clearing the discard point? probably...
			// because the discard points have to be recalculated after accumulation
			// anyway (i think).
			//qq might this want a cache to make adding quick when it has already been added
			// could it even share the cache that the collision detector uses?
			_scavengeableStreams[streamId] = new();
		}

		public StreamData GetStreamData(TStreamId streamId) {
			if (!_metadatas.TryGetValue(streamId, out var streamData))
				streamData = StreamData.Empty;
			return streamData;
		}

		public void SetStreamData(TStreamId streamId, StreamData streamData) {
			_metadatas[streamId] = streamData;
		}







		//
		//
		//

		public IEnumerable<(StreamHash, StreamData)> RelevantStreamsUncollided => throw new NotImplementedException();

		public IEnumerable<(StreamName, StreamData)> RelevantStreamsCollided => throw new NotImplementedException();

		public DiscardPoint GetDiscardPoint(StreamName streamName) {
			throw new NotImplementedException();
		}

		public DiscardPoint GetDiscardPoint(StreamHash streamHash) {
			throw new NotImplementedException();
		}

		public bool IsCollision(StreamHash streamHash) {
			throw new NotImplementedException();
		}

		public void Set(StreamName streamName, DiscardPoint dp) {
			throw new NotImplementedException();
		}

		public void Add(StreamName streamName) {
			throw new NotImplementedException();
		}
	}
}
