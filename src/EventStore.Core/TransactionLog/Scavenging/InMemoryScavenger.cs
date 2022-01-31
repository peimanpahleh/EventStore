using System;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryScavenger<TStreamId> : IScavenger {
		private readonly IAccumulator<TStreamId> _accumulator;
		private readonly ICalculator<TStreamId> _calculator;
		private readonly IExecutor<TStreamId> _executor;
		private readonly TFChunkDb _db;

		public InMemoryScavenger(
			//qq might need to be factories if we need to instantiate new when calling start()
			IAccumulator<TStreamId> accumulator,
			ICalculator<TStreamId> calculator,
			IExecutor<TStreamId> executor,
			TFChunkDb db) {

			_accumulator = accumulator;
			_calculator = calculator;
			_executor = executor;
			_db = db;
		}

		public void Start() {
			//qq this would come from the log so that we can stop/resume it.
			//qq implement stopping and resuming. at each stage. cts?
			var scavengePoint = new ScavengePoint {
				Position = _db.Config.ChaserCheckpoint.Read(),
				EffectiveNow = DateTime.Now,
			};

			_accumulator.Accumulate(scavengePoint);
			_calculator.Calculate(scavengePoint, _accumulator.ScavengeState);
			_executor.ExecuteChunks(_calculator.ScavengeInstructions);
			_executor.ExecuteIndex(_calculator.ScavengeInstructions);
			//qqqq tidy.. maybe call accumulator.done or something?
		}

		public void Stop() {
			throw new NotImplementedException(); //qq
		}
	}
}
