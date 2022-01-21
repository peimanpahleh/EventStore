using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryCalculator<TStreamId> : ICalculator<TStreamId> {
		private readonly IIndexForScavenge<TStreamId> _index;
		private readonly Dictionary<int, IChunkScavengeInstructions<TStreamId>> _instructionsByChunk =
			new();

		public InMemoryCalculator(IIndexForScavenge<TStreamId> index) {
			_index = index;
		}

		public IScavengeInstructions<TStreamId> ScavengeInstructions =>
			new InMemoryScavengeInstructions<TStreamId>();

		public void Calculate(ScavengePoint scavengePoint, IScavengeState<TStreamId> source) {
			foreach (var (streamId, streamData) in source.RelevantStreams) {
				CalculateStream(scavengePoint, streamId, streamData);
			}
		}

		private void CalculateStream(
			ScavengePoint scavengePoint,
			TStreamId streamId,
			StreamData streamData) {

			//qq fundamentally we iterate by stream because thats how the scavenge criteria are organised
			// but we want to end up with output per chunk (logical chunk to begin with) so that we can
			// decide per chunk whether it should be scavenged yet
			//qq note this means subsequent calculations will want to add to existing ChunkScavengeInstructions

			//qq so this will go something like
			// 1. figure out what is the minimum event we want to keep for this stream
			//    IMPORTANT, in a minute we are going to want to know what the _actual_ positions
			//    to discard are, so that we can attribute them to the right chunk.
			//    so its' OK to look those up as we go.
			//
			//
			// SO lets go about it a different way, lets read the stream through in slices.
			// we do need the addresses of each one, but we dont actually need the events except
			// to resolve hash collisions, so there will be optimisations here later when we
			// no longer need to resolve hash collisions. for now we can be naive and just look up
			// every event in the stream starting with the first until we get to one that we
			// dont want to discard.

			const int maxCount = 100; //qq what would be sensible?
			var fromEventNumber = 0L;
			while (true) {
				var slice = _index.ReadStreamForward(streamId, fromEventNumber, maxCount);
				//qq naive, we dont need to check every event, we could check the last one
				// and if that is to be discarded then we can discard everything in this slice.
				foreach (var evt in slice) {
					//qq important, want to keep the last event in the stream,
					// currently this isn't done in shouldkeep. add it there, or handle it out here
					// (we are reading through the stream so we know what the last one is)
					//qqqqq gotta respect the scavenge point also. do that out here or
					//  let shouldkeep do it. we want to keep everything on or after the scavenge point
					// if shouldkeep doesn't so it, rename it to indicate the scope of what it does calculate.
					if (ShouldKeep(
							scavengePoint,
							streamId,
							streamData,
							evt)) {
						// found the first one to keep. we are done discarding.
						goto Done;
					}

					Discard(streamId, evt);
				}

				if (slice.Length < maxCount) {
					//qq we discarded everything in the stream, this should never happen
					// since we always keep the last event (..unless ignore hard deletes
					// is enabled)
					// which ones are otherwise in danger of removing all the events?
					//  hard deleted?
					throw new Exception("panic"); //qq dont panic
				}


				fromEventNumber += slice.Length;
			}

			Done:;
		}

		private bool ShouldKeep(
			ScavengePoint scavengePoint,
			TStreamId streamId,
			StreamData streamData,
			EventRecord evt) {
			//qqqq important, we want to discard metadata records when they aren't the latest ones
			// for their stream. but we dont write explicit metadata records for metadata streams, so
			// we wont have accumulated it.
			// but we could say when we are calculating a stream, we could also calculate and discard
			// things from its metadta stream

			var metadata = streamData.Metadata;

			//qq check/test these carefully obviously
			if (DiscardBecauseHardDeleted(streamData, evt))
				return false;

			if (IsExpiredByTruncateBefore(metadata, evt))
				return false;

			if (IsExpiredByMaxAge(scavengePoint, metadata, evt))
				return false;

			if (IsExpiredByMaxCount(scavengePoint, streamId, metadata, evt))
				return false;

			return true;
		}

		static bool DiscardBecauseHardDeleted(
			StreamData streamData,
			EventRecord evt) {

			// keep tombstone
			return streamData.IsHardDeleted && evt.EventNumber < long.MaxValue;
		}

		static bool IsExpiredByTruncateBefore(
			StreamMetadata metadata,
			EventRecord evt) {

			if (metadata.TruncateBefore == null)
				return false;

			var expired = evt.EventNumber < metadata.TruncateBefore.Value;
			return expired;
		}

		static bool IsExpiredByMaxAge(
			ScavengePoint scavengePoint,
			StreamMetadata metadata,
			EventRecord evt) {

			if (metadata.MaxAge == null)
				return false;

			var expires = evt.TimeStamp + metadata.MaxAge;
			return expires <= scavengePoint.EffectiveNow;
		}

		bool IsExpiredByMaxCount(
			ScavengePoint scavengePoint,
			TStreamId streamId,
			StreamMetadata metadata,
			EventRecord evt) {

			if (metadata.MaxCount == null)
				return false;

			// say evt.EventNumber is 3
			// and metadata.MaxCount = 2
			// then this event will expire when we write event number 5
			// because the live events at that point will be events 4 and 5.

			var expires = evt.EventNumber + metadata.MaxCount;

			//qq the stream really should exist but what will happen here if it doesn't
			//qqqq we're going to hit this a lot, some kind of cache would be in order
			//qqqqqqqqqqqqqq BUT we are iterating through the stream so maybe 
			var lastEventNumber = _index.GetLastEventNumber(
				streamId,
				scavengePoint.Position);

			return expires <= lastEventNumber;
		}

		//qq found one to discard!
		// figure out which chunk it is for and note it down
		private void Discard(TStreamId streamId, EventRecord evt) {
			var chunkNumber = (int)(evt.LogPosition / TFConsts.ChunkSize);

			if (!_instructionsByChunk.TryGetValue(chunkNumber, out var chunkInstructions)) {
				chunkInstructions = new InMemoryChunkScavengeInstructions<TStreamId>();
				_instructionsByChunk[chunkNumber] = chunkInstructions;
			}

			chunkInstructions.Discard(
				streamId: streamId,
				//qqq considering whether this should be event number instead 
				position: evt.LogPosition,
				//qq approx. we might need an stracted sizer for the logformat actually.
				sizeInbytes: evt.Data.Length + evt.Metadata.Length);
		}
	}
}
