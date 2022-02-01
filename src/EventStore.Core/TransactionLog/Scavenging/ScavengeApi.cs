using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Chunks.TFChunk;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq consider the name
	public interface IScavenger {
		//qq probably we want this to continue a previous scavenge if there is one going,
		// or start a new one otherwise.
		void Start();
		//qq options
		// - timespan, or datetime to autostop
		// - chunk to scavenge up to
		// - effective 'now'
		// - remove open transactions : bool

		//qq probably we want this to pause a scavenge if there is one going,
		// otherwise probably do nothing.
		// in this way the user sticks with the two controls that they had before: start and stop.
		void Stop();
	}

	// the accumulator reads through the log up to the scavenge point
	// its purpose is to scope down to the streams that might need scavenging.
	public interface IAccumulator<TStreamId> {
		void Accumulate(ScavengePoint scavengePoint);
		//qq got separate apis for adding and getting state cause they'll probably be done
		// by different logical processes
		IMagicForCalculator<TStreamId> ScavengeState { get; }
	}

	//qqqq consider api. consider name
	//qq calculates the discardpoint for each relevant streams.
	// we don't calculate this during the accumulation phase because the decisionpoints would keep
	// moving as we go (e.g. time is passing for maxage, new events would move the dp on maxcount streams)
	// so to avoid doing duplicate work we dont do that until calculation phase.
	// it also means the accumulator only needs to actually process a small amount of data in the log.
	// the structure this produces is enough to quickly scavenge the chunks and ptables without
	// (typically) doing any further lookups or calculation.
	public interface ICalculator<TStreamId> {
		// processed so far.
		void Calculate(ScavengePoint scavengePoint, IMagicForCalculator<TStreamId> source);
		//qq for now having separate getter rather than returning from calculate
		// might want it to be continuous, or to maybe process ones that have been
		IScavengeInstructions<TStreamId> ScavengeInstructions { get; }
	}

	// the executor does the actual removal of the log records and index records
	// should be very rare to do any further lookups at this point.
	public interface IExecutor<TStreamId> {
		void ExecuteChunks(IScavengeInstructions<TStreamId> instructions);
		void ExecuteIndex(IScavengeInstructions<TStreamId> instructions);
	}





	public interface IChunkManagerForScavenge {
		TFChunk SwitchChunk(TFChunk chunk, bool verifyHash, bool removeChunksWithGreaterNumbers);
		TFChunk GetChunk(int logicalChunkNum);
	}

	//qq there are a couple of places we need to read chunks.
	// 1. during accumulation we need the metadata records and the timestamp of the first record in the
	//    chunk. i wonder if we should use the bulk reader.
	public interface IChunkReaderForAccumulation<TStreamId> {
		IEnumerable<RecordForAccumulator<TStreamId>> Read(
			int startFromChunk,
			ScavengePoint scavengePoint);
	}

	//qq could use streamdata? its a class though
	public abstract class RecordForAccumulator<TStreamId> {
		//qq make sure to recycle these.
		//qq prolly have readonly interfaces to implement, perhaps a method to return them for reuse
		//qq some of these are pretty similar, wil lthey end up being different in the end
		public class EventRecord : RecordForAccumulator<TStreamId> {
			public TStreamId StreamId { get; set; }
			public long LogPosition { get; set; }
		}

		public class TombStoneRecord : RecordForAccumulator<TStreamId> {
			public TStreamId StreamId { get; set; }
			public long LogPosition { get; set; }
		}

		public class MetadataRecord : RecordForAccumulator<TStreamId> {
			public TStreamId StreamId { get; set; }
			public long LogPosition { get; set; }
		}
	}

	// 2. during calculation we want to know the record sizes to determine space saving.
	//      unless we just skip this and approximate it with a record count.
	// 3. when scavenging a chunk we need to read records out of it any copy
	//    the ones we are keeping into the new chunk
	public interface IChunkReaderForScavenge<TStreamId> {
		IEnumerable<RecordForScavenge<TStreamId>> Read(TFChunk chunk);
	}

	// when scavenging we dont need all the data for a record
	//qq but we do need more data than this
	// but the bytes can just be bytes, in the end we are going to keep it or discard it.
	//qq recycle this record like the recordforaccumulation?
	public class RecordForScavenge<TStreamId> {
		public TStreamId StreamId { get; set; }
		public long EventNumber { get; set; }
	}




	//qq the purpose of this datastructure is to narrow the scope of what needs to be
	// calculated based on what we can glean by tailing the log,
	// without doubling up on what we can easily look up later.
	// for now IMagicForCalculator<TStreamId> is serving this purpose.
	//public interface IScavengeState<TStreamId> {
	//	//
	//	IEnumerable<(TStreamId, StreamData)> RelevantStreams { get; }
	//}



	//qq this contains enough information about what needs to be removed from each
	// chunk that we can decide whether to scavenge each one (based on some threshold)
	// or leave it until it has more junk in.
	// in order to figure out how much will be scavenged we probably had to do various
	// lookups. expect that we will probably may as well preserve that information so
	// that the execution itself can be done quickly, prolly without additional lookups
	//
	public interface IScavengeInstructions<TStreamId> {
		//qqqqq is chunknumber the logical chunk number?
		//qq do we want to store a separate file per logical chunk or per physical (merged) chunk.
		IEnumerable<IReadOnlyChunkScavengeInstructions<TStreamId>> ChunkInstructionss { get; }
		//qq this isn't quite it, prolly need stream name
		bool TryGetDiscardPoint(TStreamId streamId, out DiscardPoint discardPoint);
	}

	// instructions (see above) for scavenging a particular chunk.
	public interface IReadOnlyChunkScavengeInstructions<TStreamId> {
		int ChunkNumber { get; } //qq logical or phsyical?

		//qq int or long? necessarily bytes or rather accumulated weight, or maybe it can jsut be approx.
		// maybe just event count will be sufficient if it helps us to not look up records
		// currently we have to look them up anyway for hash collisions, so just run with that.
		// later we may switch to record count if it helps save lookups - or the index may even be able
		// to imply the size of the record (approximately?) once we have the '$all' stream index.
		int NumRecordsToDiscard { get; }
	}

	//qq consider if we want to use this readonly pattern for the scavenge instructions too
	public interface IChunkScavengeInstructions<TStreamId> :
		IReadOnlyChunkScavengeInstructions<TStreamId> {
		// we call this for each event that we want to discard
		// probably it is better to list what we want to discard rather than what we want to keep
		// because in a well scavenged log we will want to keep more than we want to remove
		// in a typical scavenge.

		//qq now that we dont have args here, perhaps it should be called 'increment' or similar
		void Discard();
	}

	public interface IIndexReaderForAccumulator<TStreamId> {
		//qq definitely a similar here to the delegate defined by the collision detector..
		// is it actually the same thing in need of a refactor? then the other is just a
		// decorator pattern that adds memoisation. might need to pass the hash into
		// collisiondetector.add, or let it hash it itself
		bool HashInUseBefore(ulong hash, long postion, out TStreamId candidedateCollidee);
	}


	public readonly struct IndexReadResultForScavenge {
		public readonly long LogPosition;
		public readonly long EventNumber;
	}
	//qq name
	public interface IIndexForScavenge<TStreamId> {
		//qq maxposition  / positionlimit instead of scavengepoint?
		//qq better name than 'stream'...
		long GetLastEventNumber(IndexKeyThing<TStreamId> stream, long scavengePoint);

		//qq name min age or maxage or 
		//long GetLastEventNumber(TStreamId streamId, DateTime age);

		//qq maybe we can do better than allocating an array for the return
		//qqqqq should take a scavengepoint/maxpos?
		IndexReadResultForScavenge[] ReadStreamForward(
			IndexKeyThing<TStreamId> stream,
			long fromEventNumber,
			int maxCount);
	}

	//qq consider name
	public struct IndexKeyThing {
		public static IndexKeyThing<TStreamId> CreateForHash<TStreamId>(ulong streamHash) {
			return new IndexKeyThing<TStreamId>(true, default, streamHash);
		}

		public static IndexKeyThing<TStreamId> CreateForStreamId<TStreamId>(TStreamId streamId) {
			return new IndexKeyThing<TStreamId>(true, streamId, default);
		}
	}

	//qq consider explicit layout
	public readonly struct IndexKeyThing<TStreamId> {
		public readonly bool IsHash;
		public readonly TStreamId StreamId;
		public readonly ulong StreamHash;

		public IndexKeyThing(bool isHash, TStreamId streamId, ulong streamHash) {
			IsHash = isHash;
			StreamId = streamId;
			StreamHash = streamHash;
		}
	}

	public interface ChunkTimeStampOptimisation {
		//qq we could have a dummy implemenation of this that just never kicks in
		// but could know, for each chunk, what the minimum timestamp of the records in
		// that chunk are within some range (to allow for incorrectly set clocks, dst etc)
		// then we could shortcut
		bool Foo(DateTime dateTime, long position);
	}

	public record StreamData {
		public static StreamData Empty = new(); //qq maybe dont need

		public long? MaxCount { get; init; }
		public TimeSpan? MaxAge { get; init; }
		public long? TruncateBefore { get; init; }
		//qq public long MetadataPosition { get; init; } //qq to be able to scavenge the metadata
		public bool IsHardDeleted { get; init; }
	}

	public record ScavengePoint {
		public long Position { get; set; }
		public DateTime EffectiveNow { get; set; }
	}
}
