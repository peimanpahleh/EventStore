using System;
using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq name.
	// this class efficiently stores data that very rarely but sometimes has a hash collision
	// when there is a hash collision the key is stored explicitly with the value
	// otherwise it only stores the hashes and the values
	//qqqqqq in fact, for now, perhaps we don't need to bother storing values against the collisions
	// here at all - we can just not bother scavenging those streams?
	//    at some point we will need it for gdpr, or in case a really big stream collides
	//    so really we had better at least sketch out the implementation to be sure it will work.
	public class InMemoryCollisionResolver<TKey, THash, TValue> {
		private readonly Dictionary<THash, TValue> _nonCollisions = new();
		private readonly Dictionary<TKey, TValue> _collisions = new();

		public InMemoryCollisionResolver() {
			//qq need a hasher? or maybe need a collision detector

			//qq consider that what was not a collision can be collided with.
			// do we need to move it at that point? say there are three streams that collide
			// together, could we tell which one was the one in the uncollided map
			// or would we need to store all three in the collisions.

			//qq definitely need a way to persist the non-collisions nicely
		}

		public TValue this[TKey key] {
			get => throw new NotImplementedException();
			set => throw new NotImplementedException();
		}


		public bool TryGetValue(TKey key, out TValue value) {
			throw new NotImplementedException();
		}
	}
}
