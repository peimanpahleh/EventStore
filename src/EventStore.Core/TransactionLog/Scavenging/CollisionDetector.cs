using System;
using System.Collections.Generic;
using System.Linq;

namespace EventStore.Core.TransactionLog.Scavenging {
	// add things to the collision detector and it keeps a list of things that collided.
	public class CollisionDetector<T> where T : IEquatable<T> {
		// checks if the hash is in use before the position. returns true if so.
		// if returning true then out parameter is one of the items that hashes to that hash
		public delegate bool HashInUse(T item, long positionLimit, out T candidateCollidee);

		private static EqualityComparer<T> TComparer { get; } = EqualityComparer<T>.Default;
		private readonly HashInUse _hashInUse;

		// store the values. could possibly store just the hashes instead but that would
		// lose us information and it should be so rare that there are any collisions at all.
		//qq for the real implementation make sure adding is idempotent
		private readonly HashSet<T> _collisions;
		
		//qq will need something like this to tell where to continue from. maybe not in this class though
		private long _lastPosition;

		public CollisionDetector(HashInUse hashInUse) {
			_collisions = new();
			_hashInUse = hashInUse;
		}

		public bool IsCollision(T item) => _collisions.Contains(item);
		public T[] GetAllCollisions() => _collisions.OrderBy(x => x).ToArray();

		// allllright. when we are adding an item, either:
		//   1. we have seen this stream before.
		//       then either:
		//       a. it was a collision the first time we saw it
		//           - by induction we concluded it was a collision at that time and added it to the
		//             collision list so we conclude it is a collision now
		//             TICK
		//       b. it was not a collision but has since been collided with by a newer stream.
		//           - when it was collided with we noticed and added it to the collision list so
		//             we conclude it is still a collision now
		//             NB: it is important that we added this stream to the collision list. consider:
		//                 if we look in the index to see if there are any entries for this hash
		//                 there would be, because this is the collision case
		//                 and, crucially, those entries would be a mix of this stream and other streams
		//                 because this is the 'seen before' case.
		//                 so we couldn't tell by looking up a single entry whether this was a collision
		//                 or not. BAD.
		//       c. it was not a collision and still isn't
		//           - so we look in the index to see if there are any entries for this hash
		//             there are, because we have seen this stream before
		//             all the entries are for this stream, but we only want to check one.
		//             so we check one, and we find that it is for this stream
		//             conclude no collision
		//             TICK
		//
		//   2. this is the first time we are seeing this stream.
		//       then either:
		//       a. it collides with any stream we have seen before
		//           i. it collides with 1 stream that hasnt been in a collision up til now
		//           ii. it collides with 2+ streams that are already colliding with each other.
		//           both of these cases are handled in the same way below. but if we stored the hash we
		//           could handle (ii) differently but super rare so dont bother
		//           - so we look in the index to see if there are any entries for this hash
		//             there are, because this is the colliding case.
		//             we pick any entry and look up the record
		//             the record MUST be for a different stream because this is the the first time
		//                we are seeing this stream.
		//             collision detected.
		//             add both streams to the collision list. we have their names and the hash.
		//             BUT is it possible that we are colliding with multiple streams, do we need to add
		//                    them all to the hash? it is possible yes, but they are already in the hash
		//                    because they are colliding with each other.
		//             TICK
		//       b. it does not collide with any stream we have seen before
		//           - so we look in the index to see if there are any entries for this hash
		//             there are not because this is the first time we are seeing this stream and it does
		//                not collde.
		//             conclude no collision
		//             TICK
		public void Add(T item, long position) {
			_lastPosition = position;

			if (IsCollision(item)) {
				return; // previously known collision.
			}

			// collision not previously known, but might be a new one now.
			if (!_hashInUse(item, position, out var candidateCollidee)) {
				return; // hash not in use, can be no collision.
			}

			// hash in use, but maybe by the item itself.
			if (TComparer.Equals(candidateCollidee, item)) {
				return; // no collision with oneself
			}

			// hash in use by a different item! found new collision.
			_collisions.Add(item);
			_collisions.Add(candidateCollidee);
		}
	}
}
