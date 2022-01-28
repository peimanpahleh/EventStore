using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Index.Hashes;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	class MockHahser : ILongHasher<string> {
		public ulong Hash(string x) =>
			(ulong)(x.Length == 0 ? 0 : x[0].GetHashCode());
	}

	public class CollisionDetectorTests {
		public static IEnumerable<object[]> TheCases() {
			var none = Array.Empty<string>();
			
			// the first letter of the stream name determines the the hash
			// each row is a record but we don't care much about it, just its stream name.
			yield return Case(
				"seen stream before. was a collision first time we saw it",
				("a-stream1", none),
				("a-streamOfInterest", new[] { "a-stream1", "a-streamOfInterest" }),
				("b-stream2", none),
				("a-streamOfInterest", none));

			yield return Case(
				"seen stream before. was not a collision but has since been collided with",
				("a-streamOfInterest", none),
				("a-stream1", new[] { "a-stream1", "a-streamOfInterest" }),
				("b-stream2", none),
				("a-streamOfInterest", none));

			yield return Case(
				"seen stream before. was not a collision and still isnt",
				("a-streamOfInterest", none),
				("b-stream2", none),
				("a-streamOfInterest", none));

			yield return Case(
				"first time seeing stream. no collision",
				("a-stream1", none),
				("b-stream2", none),
				("c-streamOfInterest", none));

			yield return Case(
				"first time seeing stream. collides with previous stream",
				("a-stream1", none),
				("b-stream2", none),
				("a-streamOfInterest", new[] { "a-stream1", "a-streamOfInterest" }));

			yield return Case(
				"three way collision",
				("a-stream1", none),
				("a-stream2", new[] { "a-stream1", "a-stream2" }),
				("a-stream3", new[] { "a-stream3" }));

			yield return Case(
				"in combination",
				("a-stream1", none), // 2b
				("a-stream2", new[] { "a-stream1", "a-stream2" }), // 2a
				("b-stream3", none),
				("a-stream4", new[] { "a-stream4" }),
				("b-stream3", none), // 1c
				("a-stream1", none), // 1b
				("a-stream2", none)); // 1a

			static object[] Case(string name, params (string, string[])[] data) {
				return new object[] {
					name, data
				};
			}
		}

		[Theory]
		[MemberData(nameof(TheCases))]
		public void Works(string caseName, (string StreamName, string[] NewCollisions)[] data) {
			Assert.NotNull(caseName);

			var hasher = new MockHahser();

			var index = new Dictionary<ulong, List<int>>();
			for (var i = 0; i < data.Length; i++) {
				var streamName = data[i].StreamName;
				var hash = hasher.Hash(streamName);
				if (!index.TryGetValue(hash, out var entries)) {
					entries = new();
					index[hash] = entries;
				}
				entries.Add(i);
			}

			// maps hashes to stream names
			var cache = new Dictionary<ulong, string>();

			// simulates looking in the index for records with the current hash up to
			// the all stream positionLimit. exclude the positionLimit itself because that is the
			// position of the record that the hash came from.
			//qq the index version of this wont be that expensive, but wont be that cheap either
			// and we'll be calling it for every record. so we could definitely do with a cache
			// as long as that doesn't break any of the properties that we require
			bool HashInUse(string streamName, long positionLimit, out string candidateCollidee) {
				var hash = hasher.Hash(streamName);

				if (cache.TryGetValue(hash, out candidateCollidee))
					return true;

				// simulate hitting the ptables. if we find any entries for this hash
				// then look up the record for one of them to get the stream name
				if (index.TryGetValue(hash, out var entries)) {
					foreach (var entry in entries) {
						if (entry < positionLimit) {
							var candidate = data[entry].StreamName;
							candidateCollidee = candidate;
							cache[hash] = candidate;
							return true;
						}
					}
				}

				cache[hash] = streamName;
				candidateCollidee = default;
				return false;
			}

			var sut = new CollisionDetector<string>(HashInUse);


			var expectedCollisions = new HashSet<string>();

			for (var i = 0; i < data.Length; i++) {
				foreach (var newCollision in data[i].NewCollisions)
					expectedCollisions.Add(newCollision);

				sut.Add(data[i].StreamName, i);
				Assert.Equal(
					expectedCollisions.OrderBy(x => x),
					sut.GetAllCollisions());
			}
		}
	}
}
