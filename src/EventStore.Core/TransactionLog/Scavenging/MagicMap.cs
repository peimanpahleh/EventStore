using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	public readonly struct DiscardPoint {
		public readonly long Value;

		//qq depends on whether the DP is the first event to keep
		// or the last event to discard. which in turn will depend on which is easier to generate
		public bool ShouldDiscard(long eventNumber) =>
			eventNumber < Value;
	}

	public readonly struct StreamHash {
		public readonly long Value;
	}

	public readonly struct StreamName {
		public readonly string Value;
	}

	// there are two kinds of streams that we might want to remove events from
	//    - User streams with metadata.
	//    - Metadata streams.
	//
	// however, we need to know about _all_ the stream collisions in the database not just the ones
	// that we might remove events from, so that later we can scavenge the index without looking anything
	// up in the log.

	// Together these are the scavengable streams. We need a DiscardPoint for each.
	// We only need to store metadata for the user streams with metadata since the metadata for metadatastreams is implicit.

	// accumulator iterates through the log, spotting metadata records
	// put in the data that the chunk and ptable scavenging require
	public interface IMagicForAccumulator<TStreamId> {
		//qq maybe prefer passing in a single
		//datastructure to show that the position is that of the record
		void Add(TStreamId streamId, long position);

		// don't need to call this for both normal streams and corresponding metadata streams
		// this also implies that the corresponding metadata stream is relevant to the scavenge.
		//
		// this needs to spot if there is a hash collision. can it do it? bear in mind that it can be
		// called multiple times for the same stream.
		// 1. hash the stream name, see if we already have a record for that stream.
		//         (check the (hash -> name) cache first) (check the collisions first also?)
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
		//                - we can if we need to because we have both stream names and stream datas here.
		//                  that will do for now.
		//    - else (no record)
		//        - just add it
		void Set(TStreamId streamId, StreamData streamData);
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

	public interface IMagicForExecutor {
		bool IsCollision(StreamHash streamHash);
		DiscardPoint GetDiscardPoint(StreamName streamName);
		DiscardPoint GetDiscardPoint(StreamHash streamHash);
	}
}
//qq note, probably need to complain if the ptable is a 32bit table
//qq maybe we need a collision free core that is just a map
//qq just a thought, lots of the metadatas might be equal, we might be able to store each unique instance once. implementation detail.
