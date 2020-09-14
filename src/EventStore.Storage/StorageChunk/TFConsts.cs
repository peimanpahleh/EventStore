﻿using System;

namespace EventStore.Core.Services.Storage.StorageChunk {
	public class TFConsts {
		public const int MaxLogRecordSize = 16 * 1024 * 1024; // 16Mb ought to be enough for everything?.. ;)
		public const int LogRecordOverheadSize = 10000;
		public const int EffectiveMaxLogRecordSize = MaxLogRecordSize - LogRecordOverheadSize;
		public const int MidpointsDepth = 10;

		public const int ChunkSize = 256 * 1024 * 1024;
		public const int ChunkHeaderSize = 128;
		public const int ChunkFooterSize = 128;
		public const int ChunksCacheSize = 2 * (ChunkSize + ChunkHeaderSize + ChunkFooterSize);

		internal const int ScavengerMaxThreadCount = 4;

		public static TimeSpan MinFlushDelayMs = TimeSpan.FromMilliseconds(2);
	}
}
