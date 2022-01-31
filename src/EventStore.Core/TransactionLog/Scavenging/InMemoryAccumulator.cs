using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryAccumulator<TStreamId> : IAccumulator<TStreamId> {
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


		//qq condider what requirements this has of the chunkreader in terms of transactions
		//qq are we expecting to read only committed records?
		//qq are we expecting to read the records in commitPosition order?
		//     (if so bulkreader might not be ideal)
		//       or prepareposition order
		//qq in fact we should probably do a end to end ponder of transactions
		public void Accumulate(ScavengePoint scavengePoint) {
			var records = _chunkReader.Read(startFromChunk: 0, scavengePoint);
			foreach (var record in records) {
				switch (record) {
					case RecordForAccumulator<TStreamId>.EventRecord x:
						Accumulate(x);
						break;
					case RecordForAccumulator<TStreamId>.MetadataRecord x:
						Accumulate(x);
						break;
					case RecordForAccumulator<TStreamId>.TombStoneRecord x:
						Accumulate(x);
						break;
					default:
						throw new NotImplementedException(); //qq
				}
			}
		}

		private void Accumulate(RecordForAccumulator<TStreamId>.EventRecord record) {
			//qq hmm does this need to be the prepare log position, the commit log position, or, in fact, both?
			_magic.NotifyForCollisions(record.StreamId, record.LogPosition);
		}

		private void Accumulate(RecordForAccumulator<TStreamId>.MetadataRecord record) {
			_magic.NotifyForCollisions(record.StreamId, record.LogPosition);

			var originalStream = _metastreamLookup.OriginalStreamOf(record.StreamId);

			_magic.NotifyForScavengeableStreams(record.StreamId);
			_magic.NotifyForScavengeableStreams(originalStream);

			var streamData = _magic.GetStreamData(originalStream);
			//qqqq set the new stream data, leave the harddeleted flag alone.
			// consider if streamdata really wants to be immutable. also c# records not supported in v5
			var newStreamData = streamData with {
				MaxAge = null,
				MaxCount = 345,
				TruncateBefore = 567,
			};
			_magic.SetStreamData(originalStream, newStreamData);
		}

		private void Accumulate(RecordForAccumulator<TStreamId>.TombStoneRecord record) {
			_magic.NotifyForCollisions(record.StreamId, record.LogPosition);
			_magic.NotifyForScavengeableStreams(record.StreamId);

			var streamData = _magic.GetStreamData(record.StreamId);
			var newStreamData = streamData with { IsHardDeleted = true };
			_magic.SetStreamData(record.StreamId, newStreamData);
		}

		private void AccumulateTimeStamps(int ChunkNumber, DateTime createdAt) {
			//qq call this. consider name
			// actually make this add to magicmap, separate datastructure for the timestamps
			// idea is to decide whether a record can be discarded due to maxage just
			// by looking at its logposition (i.e. index-only)
			// needs configurable leeway for clockskew
		}
	}
}
