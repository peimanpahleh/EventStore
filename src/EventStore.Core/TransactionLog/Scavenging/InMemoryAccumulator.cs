using System;
using System.Collections.Generic;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryAccumulator<TStreamId> : IAccumulator<TStreamId> {
		//qq prolly want to use the chunk bulk reader in here to avoid having to instantiate the records
		//qq what state does this want to accumulate
		private readonly Dictionary<TStreamId, StreamData> _dict =
			new(EqualityComparer<TStreamId>.Default);
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkReaderForAccumulation<TStreamId> _reader;

		public InMemoryAccumulator(
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkReaderForAccumulation<TStreamId> reader) {

			_metastreamLookup = metastreamLookup;
			_reader = reader;
		}

		//qq prolly dont need to keep
		public IScavengeState<TStreamId> ScavengeState => new InMemoryScavengeState<TStreamId>(_dict);


		public void Accumulate(ScavengePoint scavengePoint) {
			//qq we do need to do something for _every_ record so that we can get a full list of the hash collisions.

			var records = _reader.Read(startFromChunk: 0, scavengePoint);
			foreach (var record in records) {
				switch (record) {
					case RecordForAccumulator<TStreamId>.Metadata x:
						Add(x);
						break;
					case RecordForAccumulator<TStreamId>.TombStone x:
						Add(x);
						break;
					case RecordForAccumulator<TStreamId>.TimeStampMarker x:
						Add(x);
						break;
					default:
						throw new NotImplementedException(); //qq
				}

			}
		}

		private void Add(RecordForAccumulator<TStreamId>.Metadata metadata) {
			//qqqqqqqqqq actually make this add, polly use the magic map
			var streamToScavenge = _metastreamLookup.OriginalStreamOf(metadata.StreamId);
		}

		private void Add(RecordForAccumulator<TStreamId>.TombStone tombstone) {
			//qqqqqqqqqq actually make this add, polly use the magic map
			// set harddeleted on the streamdata
			var streamToScavenge = tombstone.StreamId;
			GetDataForStream(streamToScavenge, out var data);
			var newData = data with { IsHardDeleted = true };
			SetDataForStream(streamToScavenge, newData);
			return;
		}

		private void Add(RecordForAccumulator<TStreamId>.TimeStampMarker marker) {
			//qqqqqqqqqq actually make this add, separate datastructure for the timestamps
		}

		private void GetDataForStream(TStreamId streamId, out StreamData streamData) {
			if (_dict.TryGetValue(streamId, out streamData))
				return;

			streamData = StreamData.Empty;
		}

		private void SetDataForStream(TStreamId streamId, StreamData streamData) {
			_dict[streamId] = streamData;
		}
	}
}
