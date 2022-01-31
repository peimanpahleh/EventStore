using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryAccumulator<TStreamId> : IAccumulator<TStreamId> {
		//qq prolly want to use the chunk bulk reader in here to avoid having to instantiate the records
		//qq what state does this want to accumulate
		private readonly Dictionary<TStreamId, StreamData> _dict =
			new(EqualityComparer<TStreamId>.Default);
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkReaderForAccumulation<TStreamId> _chunkReader;
		private readonly IMagicForAccumulator<TStreamId> _magic;

		public InMemoryAccumulator(
			ILongHasher<TStreamId> hasher,
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkReaderForAccumulation<TStreamId> chunkReader,
			IIndexReaderForAccumulator<TStreamId> indexReader) {

			_metastreamLookup = metastreamLookup;
			_chunkReader = chunkReader;
			_magic = new InMemoryMagicMap<TStreamId>(hasher, indexReader);
		}

		//qq prolly dont need to keep
		public IScavengeState<TStreamId> ScavengeState => new InMemoryScavengeState<TStreamId>(_dict);


		public void Accumulate(ScavengePoint scavengePoint) {
			//qq we do need to do something for _every_ record so that we can get a full list of the hash collisions.

			var records = _chunkReader.Read(startFromChunk: 0, scavengePoint);
			foreach (var record in records) {
				switch (record) {
					case RecordForAccumulator<TStreamId>.StandardRecord x:
						Accumulate(x);
						break;
					case RecordForAccumulator<TStreamId>.Metadata x:
						Accumulate(x);
						break;
					case RecordForAccumulator<TStreamId>.TombStone x:
						Accumulate(x);
						break;
					case RecordForAccumulator<TStreamId>.TimeStampMarker x:
						Accumulate(x);
						break;
					default:
						throw new NotImplementedException(); //qq
				}

			}
		}

		private void Accumulate(RecordForAccumulator<TStreamId>.StandardRecord record) {
			_magic.Add(record.StreamId, record.LogPosition);
			//qqqqqqqqqq actually make this add, polly use the magic map
		}

		private void Accumulate(RecordForAccumulator<TStreamId>.Metadata metadata) {
			//qqqqqqqqqq actually make this add, polly use the magic map
			var streamToScavenge = _metastreamLookup.OriginalStreamOf(metadata.StreamId);
		}

		private void Accumulate(RecordForAccumulator<TStreamId>.TombStone tombstone) {
			//qqqqqqqqqq actually make this add, polly use the magic map
			// set harddeleted on the streamdata
			var streamToScavenge = tombstone.StreamId;
			GetDataForStream(streamToScavenge, out var data);
			var newData = data with { IsHardDeleted = true };
			SetDataForStream(streamToScavenge, newData);
			return;
		}

		private void Accumulate(RecordForAccumulator<TStreamId>.TimeStampMarker marker) {
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
