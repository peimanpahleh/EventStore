using System;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryScavenger<TStreamId> : IScavenger {
		private readonly TFChunkDb _db;
		private readonly IReadIndex<TStreamId> _readIndex;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkManagerForScavenge _chunkManager;
		private readonly IChunkReaderForAccumulation<TStreamId> _chunkReaderForAccumulation;
		private readonly IChunkReaderForScavenge<TStreamId> _chunkReaderForScavenge;

		public InMemoryScavenger(
			TFChunkDb db,
			IReadIndex<TStreamId> readIndex,
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkManagerForScavenge chunkManager,
			IChunkReaderForAccumulation<TStreamId> chunkReaderForAccumulation,
			IChunkReaderForScavenge<TStreamId> chunkReaderForScavenge) {

			_db = db;
			_readIndex = readIndex;
			_metastreamLookup = metastreamLookup;
			_chunkManager = chunkManager;
			_chunkReaderForAccumulation = chunkReaderForAccumulation;
			_chunkReaderForScavenge = chunkReaderForScavenge;
		}

		public void Start() {
			//qq tbd: what do we want to do on demand when we scavenge, what do we want to be
			// doing continuously as we go
			// but for now lets do it all here although we will keep the components separate.
			// we will also keep everything in memory for now too rather than on disk.
			// in this way we hope to feel out the right shape of the solution.

			//qq should these be constructed on start or injected, prolly injected
			IAccumulator<TStreamId> accumulator = new InMemoryAccumulator<TStreamId>(
				_metastreamLookup,
				_chunkReaderForAccumulation);

			ICalculator<TStreamId> calculator = new InMemoryCalculator<TStreamId>(
				new IndexForScavenge<TStreamId>(_readIndex));

			IExecutor<TStreamId> executor = new InMemoryExecutor<TStreamId>(
				_chunkManager,
				_chunkReaderForScavenge);

			//qq this would come from the log so that we can stop/resume it.
			var scavengePoint = new ScavengePoint {
				Position = _db.Config.ChaserCheckpoint.Read(),
				EffectiveNow = DateTime.Now,
			};

			accumulator.Accumulate(scavengePoint);
			calculator.Calculate(scavengePoint, accumulator.ScavengeState);
			executor.Execute(calculator.ScavengeInstructions);
			//qqqq tidy.. maybe call accumulator.done or something?
		}

		public void Stop() {
			throw new NotImplementedException();
		}
	}
}
