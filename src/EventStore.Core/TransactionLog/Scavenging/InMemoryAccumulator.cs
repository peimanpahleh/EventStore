using System;
using System.Collections.Generic;
using EventStore.Core.Data;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryAccumulator<TStreamId> : IAccumulator<TStreamId> {
		//qq what state does this want to accumulate
		private readonly Dictionary<TStreamId, StreamData> _dict = new(EqualityComparer<TStreamId>.Default);
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		public InMemoryAccumulator(IMetastreamLookup<TStreamId> metastreamLookup) {
			_metastreamLookup = metastreamLookup;
		}

		//qq prolly dont need to keep
		public IScavengeState<TStreamId> ScavengeState => new InMemoryScavengeState<TStreamId>(_dict);

		public void Dispose() {
			throw new NotImplementedException();
		}

		public void InitializeWithConfirmed(INameLookup<TStreamId> source) {
			throw new NotImplementedException();
		}

		public void Confirm(
			IList<IPrepareLogRecord<TStreamId>> replicatedPrepares,
			bool catchingUp,
			IIndexBackend<TStreamId> backend) {

			//qq possibly we could/should assume they are all for the same stream
			// rather than treating them individually.

			foreach (var prepare in replicatedPrepares) {
				Add(prepare);
			}
		}

		public void Confirm(
			IList<IPrepareLogRecord<TStreamId>> replicatedPrepares,
			CommitLogRecord commit,
			bool catchingUp,
			IIndexBackend<TStreamId> backend) {

			//qq need to do anything in here?
		}

		private void Add(IPrepareLogRecord<TStreamId> prepare) {
			//qq for now accumulating only metadata and tombstones.
			// might even want to accumulate only specific metadata (we dont care about acls
			// for example)

			if (IsMetadata(prepare)) {
				var streamToScavenge = _metastreamLookup.OriginalStreamOf(prepare.EventStreamId);
				GetDataForStream(streamToScavenge, out var data);
				//qqq seeing as we have the actual metadata here, it might be sensible
				// to note down what kind of filters are in the metadata perhaps, or maybe the whole metadata
				// depending on the space/time tradeoffs that emerge.

				//qq refactor with GetStreamMetadataUncached
				var metadata = StreamMetadata.TryFromJsonBytes(prepare);
				var newData = data with { Metadata = metadata };
				SetDataForStream(streamToScavenge, newData);
				return;
			}

			if (IsTombStone(prepare)) {
				var streamToScavenge = prepare.EventStreamId;
				GetDataForStream(streamToScavenge, out var data);
				var newData = data with { IsHardDeleted = true };
				SetDataForStream(streamToScavenge, newData);
				return;
			}
		}

		private void GetDataForStream(TStreamId streamId, out StreamData streamData) {
			if (_dict.TryGetValue(streamId, out streamData))
				return;

			streamData = StreamData.Empty;
		}

		private void SetDataForStream(TStreamId streamId, StreamData streamData) {
			_dict[streamId] = streamData;
		}

		//qq think of a better name for TStreamId
		private static bool IsMetadata(IPrepareLogRecord<TStreamId> prepare) {
			// prepare.EventType comparer equals metadataeventtype
			return true; //qqqq
		}

		private static bool IsTombStone(IPrepareLogRecord<TStreamId> prepare) {
			return prepare.ExpectedVersion + 1 == long.MaxValue;
		}

	}
}
