using System;
using System.Collections.Generic;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryScavenger<TStreamId> : IScavenger {
		private readonly TFChunkDb _db;
		private readonly IReadIndex<TStreamId> _readIndex;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;

		public InMemoryScavenger(
			TFChunkDb db,
			IReadIndex<TStreamId> readIndex,
			IMetastreamLookup<TStreamId> metastreamLookup) {

			_db = db;
			_readIndex = readIndex;
			_metastreamLookup = metastreamLookup;
		}

		//qq probably we want this to continue a previous scavenge if there is one going, or start a new one otherwise.
		public void Start() {
			//qq tbd: what do we want to do on demand when we scavenge, what do we want to be doing continuously as we go
			// but for now lets do it all here although we will keep the components separate.
			// we will also keep everything in memory for now too rather than on disk.
			// in this way we hope to feel out the right shape of the solution.

			//qq should these be constructed on start or injected
			IAccumulator<TStreamId> accumulator = new InMemoryAccumulator<TStreamId>(_metastreamLookup);
			ICalculator<TStreamId> calculator = new InMemoryCalculator<TStreamId>(
				new IndexForScavenge<TStreamId>(_readIndex));
			IExecutor<TStreamId> executor = new InMemoryExecutor<TStreamId>();

			PopulateAccumulator(accumulator, _db);

			//qq this would come from the log so that we can stop/resume it.
			var scavengePoint = new ScavengePoint {
				Position = _db.Config.ChaserCheckpoint.Read(),
				EffectiveNow = DateTime.Now,
			};

			calculator.Calculate(scavengePoint, accumulator.ScavengeState);
			executor.Execute(calculator.ScavengeInstructions);
			//qqqq tidy.. maybe call accumulator.done or something?
		}

		//qq probably we want this to pause a scavenge if there is one going, otherwise probably do nothing.
		// in this way the user sticks with the two controls that they had before: start and stop.
		public void Stop() {
			throw new NotImplementedException();
		}

		//qq later this could likely be done continuously as part of index committing?
		// if so we get cheap filtering then because we have it loaded into memory _anyway_
		private static void PopulateAccumulator(IAccumulator<TStreamId> accumulator, TFChunkDb db) {
			//qq if we do end up keeping some sort of on-demand catchup rather than continuous accumulation
			// then we could consider adding some mechanism that doesn't load the whole record
			// or maybe drive it by reading a projection stream... but it seems a bit odd to add more
			// stuff to the database to help take things out. index only stream would be ideal
			// in fact, if we could add an index on demand that did something like keep the last event for
			// a certain set of streams?

			// for now just naive iterate through. if we stick with this we want to be able to
			// cancel it mid chunk. (old TraverseChunkBasic does this)
			IEnumerable<TFChunk> GetAllChunks(int startFromChunk) {
				var scavengePosition = db.Config.ChunkSize * (long)startFromChunk;
				var scavengePoint = db.Config.ChaserCheckpoint.Read();
				while (scavengePosition < scavengePoint) {
					var chunk = db.Manager.GetChunkFor(scavengePosition);
					if (!chunk.IsReadOnly) {
						yield break;
					}

					yield return chunk;

					scavengePosition = chunk.ChunkHeader.ChunkEndPosition;
				}
			}

			void TraverseChunkBasic(TFChunk chunk) {
				var result = chunk.TryReadFirst();
				var singletonArray = new IPrepareLogRecord<TStreamId>[1]; //qq stackalloc if we keep this
				while (result.Success) {
					//qq do we need to check if this is committed...
					// can you change metadata in a transaction?
					// can you tombstone in a transaction?
					// this probably doesn't matter if we switch this to continuous.
					//qq letting the accumulator decide what to keep
					//qq should we batch them togther or is presenting one at a time fine
					if (result.LogRecord is IPrepareLogRecord<TStreamId> prepare) {
						singletonArray[0] = prepare;
						accumulator.Confirm(
							replicatedPrepares: singletonArray,
							catchingUp: false, //qq hmm
							backend: null); //qq hmm
					}

					result = chunk.TryReadClosestForward(result.NextPosition);
				}
			}

			foreach (var chunk in GetAllChunks(startFromChunk: 0)) {
				TraverseChunkBasic(chunk);
			}
		}
	}
}
