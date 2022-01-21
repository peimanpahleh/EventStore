using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq consider the name
	public interface IScavenger {
		void Start();
		//qq options
		// - timespan, or datetime to autostop
		// - chunk to scavenge up to
		// - effective 'now'
		// - remove open transactions : bool
		void Stop();
	}

	public interface IAccumulator<TStreamId> : INameIndexConfirmer<TStreamId> {
		//qq got separate apis for adding and getting state cause they'll probably be done
		// by different logical processes
		IScavengeState<TStreamId> ScavengeState { get; }
	}

	//qqqq consider api. consider name
	//qq this takes the accumulated scavenge state and uses it, in combination with the
	// index/log/etc to calculate what to scavenge per chunk.
	public interface ICalculator<TStreamId> {
		// processed so far.
		void Calculate(ScavengePoint scavengePoint, IScavengeState<TStreamId> source);
		//qq for now having separate getter rather than returning from calculate
		// might want it to be continuous, or to maybe process ones that have been
		IScavengeInstructions<TStreamId> ScavengeInstructions { get; }
	}

	public interface IExecutor<TStreamId> {
		void Execute(IScavengeInstructions<TStreamId> instructions);
	}












	//qq the purpose of this datastructure is to narrow the scope of what needs to be
	// calculated based on what we can glean by tailing the log,
	// without doubling up on what we can easily look up later.
	public interface IScavengeState<TStreamId> {
		IEnumerable<(TStreamId, StreamData)> RelevantStreams { get; }
	}

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

		IIndexScavengeInstructions IndexInstructions { get; }
	}

	// instructions (see above) for scavenging a particular chunk.
	//
	// 
	public interface IReadOnlyChunkScavengeInstructions<TStreamId> {
		int ChunkNumber { get; } //qq logical or phsyical?

		//qq or long? necessarily bytes or rather accumulated weight, or maybe it can jsut be approx.
		// maybe just event count will be sufficient if it helps us to not look up records
		// currently we have to look them up anyway for hash collisions, so just run with that.
		// later we may switch to record count if it helps save lookups - or the index may even be able to
		// imply the size of the record (approximately?) once we have the '$all' stream index.
		int BytesToSave { get; }

		//qq keep per stream the earliest event that we want to keep
		// will this work for the duplicate events out of order bug where there is an extra event 0 later on
		// or will we need to adapt for that, or detect it and complain, or assume it is rectified in advance
		//qqq it might be nicer if this was a position but that might not work because of the above bug
		IDictionary<TStreamId, long> EarliestEventsToKeep { get; set; } //qq name

		//qqqqqqqqqqqqqqqqqq to figure out if it might be better to explicitly write down all the event positions to keep/discard
	}

	//qq consider if we want to use this readonly pattern for the scavenge instructions too
	public interface IChunkScavengeInstructions<TStreamId> : IReadOnlyChunkScavengeInstructions<TStreamId> {
		//qq position or event number.. this will become clearer when we come to consume it.
		// the position is more useful to the index
		void Discard(TStreamId streamId, long position, int sizeInbytes);
	}


	public interface IIndexScavengeInstructions {
		//qq not per ptable because they change, but this could store something
		// to make it more efficient to scavenge a ptable. 
		// we might want to scavenge the ptable as its own operation, or we might want
		// to fold it into the merge since thats when we are going 
	}

	//qq name
	public interface IIndexForScavenge<TStreamId> {
		//qqqqqq probably get rid of these two, just reading the stream forwards is enough
		// these get the last event number for a stream according to particular bounds
		long GetLastEventNumber(TStreamId streamId, long scavengePoint);

		//qq name min age or maxage or 
		//long GetLastEventNumber(TStreamId streamId, DateTime age);

		EventRecord[] ReadStreamForward(TStreamId streamId, long fromEventNumber, int maxCount);
	}

	public interface ChunkTimeStampOptimisation {
		//qq we could have a dummy implemenation of this that just never kicks in
		// but could know, for each chunk, what the minimum timestamp of the records in
		// that chunk are within some range (to allow for incorrectly set clocks, dst etc)
		// then we could shortcut
		bool Foo(DateTime dateTime, long position);
	}

	public record StreamData {
		public static StreamData Empty = new();
		//qq this would just want to be maxcount, maxage, tb to be fixed size
		public StreamMetadata Metadata { get; init; }
		public bool IsHardDeleted { get; init; }
	}

	public record ScavengePoint {
		public long Position { get; set; }
		public DateTime EffectiveNow { get; set; }
	}
}
