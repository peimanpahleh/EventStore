using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.DataStructures.ProbabilisticFilter.MemoryMappedFileBloomFilter;
using EventStore.Core.TransactionLog.Checkpoint;
using Serilog;
using Checkpoint = System.Int64;

namespace EventStore.Core.LogAbstraction.Common {
	public class StreamNameExistenceFilter :
		INameExistenceFilter<Checkpoint> {
		private readonly string _filterName;
		private readonly Encoding _utf8NoBom = new UTF8Encoding(false, true);
		private readonly MemoryMappedFileStringBloomFilter _mmfStringBloomFilter;
		private readonly MemoryMappedFileCheckpoint _checkpoint;
		private readonly Debouncer _checkpointer;
		private readonly CancellationTokenSource _cancellationTokenSource;

		protected static readonly ILogger Log = Serilog.Log.ForContext<StreamNameExistenceFilter>();

		public StreamNameExistenceFilter(
			string directory,
			string filterName,
			long size,
			TimeSpan checkpointInterval) {
			_filterName = filterName;

			if (!Directory.Exists(directory)) {
				Directory.CreateDirectory(directory);
			}

			var bloomFilterFilePath = $"{directory}/{_filterName}.dat";
			var checkpointFilePath =  $"{directory}/{_filterName}.chk";

			try {
				_mmfStringBloomFilter = new MemoryMappedFileStringBloomFilter(bloomFilterFilePath, size);
			} catch (CorruptedFileException exc) {
				Log.Error(exc, "{filterName} is corrupted. Rebuilding...", _filterName);
				File.Delete(bloomFilterFilePath);
				File.Delete(checkpointFilePath);
				_mmfStringBloomFilter = new MemoryMappedFileStringBloomFilter(bloomFilterFilePath, size);
			}

			Log.Information("{filterName} has successfully loaded.", _filterName);
			Log.Debug("Optimal maximum number of items that fits in the {filterName} with a configured size of " +
			                "{size:N0} MB is approximately equal to: {n:N0} with false positive probability: {p:N2}",
				_filterName,
				size / 1000 / 1000,
				_mmfStringBloomFilter.OptimalMaxItems,
				_mmfStringBloomFilter.FalsePositiveProbability);

			_checkpoint = new MemoryMappedFileCheckpoint(checkpointFilePath, _filterName, true);

			_cancellationTokenSource = new();
			_checkpointer = new Debouncer(
				checkpointInterval,
				token => {
					try {
						_mmfStringBloomFilter.Flush();
						_checkpoint.Flush();
						Log.Debug("{filterName} took checkpoint at position: {position}", _filterName, _checkpoint.Read());
					} catch (Exception ex) {
						Log.Error(ex, "{filterName} could not take checkpoint at position: {position}", _filterName, _checkpoint.Read());
					}
					return Task.CompletedTask;
				}, _cancellationTokenSource.Token);

		}

		public void Initialize(INameEnumerator<Checkpoint> source) {
			var lastCheckpoint = _checkpoint.Read();
			foreach (var (name, checkpoint) in source.EnumerateNames(lastCheckpoint)) {
				Add(name, checkpoint);
			}
		}

		public void Add(string name, Checkpoint checkpoint) {
			_mmfStringBloomFilter.Add(name);
			Log.Verbose("{filterName} added new entry: {name}", _filterName, name);
			_checkpoint.Write(checkpoint);
			_checkpointer.Trigger();
		}

		public bool? Exists(string name) => !_mmfStringBloomFilter.MayExist(name) ? false : null;

		public void Dispose() {
			_cancellationTokenSource?.Cancel();
			_mmfStringBloomFilter?.Dispose();
			GC.SuppressFinalize(this);
		}
	}
}
