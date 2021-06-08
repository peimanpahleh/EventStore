using System;

namespace EventStore.Core.DataStructures.ProbabilisticFilter {
	//qq suspicious that we're not accessing the implementations through these interfaces
	public interface IProbabilisticFilter<in TItem> {
		void Add(TItem item);
		bool MayExist(TItem item);
	}

	public interface IProbabilisticFilter {
		void Add(ReadOnlySpan<byte> item);
		bool MayExist(ReadOnlySpan<byte> item);
	}
}