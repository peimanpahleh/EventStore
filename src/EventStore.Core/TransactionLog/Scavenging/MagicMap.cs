using System.Collections.Generic;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {
	public readonly struct DiscardPoint {
		public readonly long Value;
	}

	public readonly struct StreamHash {
		public readonly long Value;
	}

	public readonly struct StreamName {
		public readonly string Value;
	}

	// there are two kinds of streams relevant to the scavenge.
	//    - User streams with metadata.
	//    - Metadata streams.
	//
	// Together these are the relevant streams. We need a DiscardPoint for each.
	// We only need to store metadata for the user streams with metadata.
	//   The Relevant Streams are any stream that we might need to remove events from.
	//   Then there is a subset of those streams that have explicit metadata.

	// accumulator iterates through the log, spotting metadata records
	// put in the data that the chunk and ptable scavenging require
	public interface IMagicForAccumulator {
		// don't need to call this for both normal streams and corresponding metadata streams
		// this also implies that the corresponding metadata stream is relevant to the scavenge.
		//
		// this needs to spot if there is a hash collision. can it do it? bear in mind that it can be called
		// multiple times for the same stream.
		// 1. hash the stream name, see if we already have a record for that stream. (check the (hash -> name) cache first) (check the collisions first also?)
		//    - if we do have a record for the hash
		//        - if that record has the same stream name
		//            - means we need to store the address of the metadata record to get the name
		//            - (make sure we dont scavenge that record while referencing it.. or if we legit can,
		//               that something sensible happens)
		//            - just update it. no collision.
		//            - (note we can cache the (hash -> stream name) lookup, populate it as we call Set as well)
		//        - else (different stream name)
		//            - collision detected!
		//            - store the streamName and streamData in another datastructure (collision structure)
		//            - do we need to pull the one that got collided with out into the collision structure?
		//                - we can if we need to because we have both stream names and stream datas here. that will do for now.
		//    - else (no record)
		//        - just add it
		void Set(StreamName streamName, StreamData streamData);
	}

	public interface IMagicForCalculator {
		// Calculator iterates through the relevant streams and their metadata
		//qq note we dont have to _store_ the metadatas for the metadatastreams internally, we could store them separately.
		//qq if this ever changes to return the stream name then we're in trouble cause it means we'd have to store them.
		IEnumerable<(StreamHash, StreamData)> RelevantStreamsUncollided { get; }
		IEnumerable<(StreamName, StreamData)> RelevantStreamsCollided { get; }

		//qq we set a discard point for every relevant stream.
		//qq we definitely need a streamname overload because there might be hash collisions and we need to store for both
		void Set(StreamName streamName, DiscardPoint dp);
		// but we might only _have_ the stream name for streams that collided. if so have a streamhash overload too:
		// void Set(StreamHash streamHash, DiscardPoint dp);
	}



	// Then the executor:

	public interface IMagicForScavengingChunk {
		bool IsCollision(StreamHash streamHash);
		DiscardPoint GetDiscardPoint(StreamName streamName);
	}

	public interface IMagicForScavengingPtable {
		bool IsCollision(StreamHash streamHash);
		DiscardPoint GetDiscardPoint(StreamHash streamHash);
	}

	public class MagicMap :
		IMagicForAccumulator,
		IMagicForCalculator,
		IMagicForScavengingChunk,
		IMagicForScavengingPtable {

		//qq private readonly Dictionary<long, long> _dict;
		private readonly ILongHasher<long> _hasher;

		public MagicMap(ILongHasher<long> hasher) {
			_hasher = hasher;
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

		public void Set(StreamName streamName, StreamData streamData) {
			throw new System.NotImplementedException();
		}

		public void Set(StreamName streamName, DiscardPoint dp) {
			throw new System.NotImplementedException();
		}
	}
}
//qq note, probably need to complain if the ptable is a 32bit table
//qq maybe we need a collision free core that is just a map
//qq just a thought, lots of the metadatas might be equal, we might be able to store each unique instance once. implementation detail.
