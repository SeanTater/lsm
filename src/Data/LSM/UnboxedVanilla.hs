{-|
  This module implements a fast, ordered, cache-friendly, persistent, functional
  in-memory log structured merge tree, which is appropriate for sets and maps
  that focus on writes rather than reads.
-}
module Data.LSM.UnboxedVanilla (
  -- * A traditional map based on LSM
  UnboxedVanillaMap,
  singleton,
  put,
  get,
  fromList,
  toList
) where
import Data.LSM (KV(..), Last(..))
import qualified Data.LSM as LSM
import Data.Vector.Unboxed (Vector, Unbox)
import Data.LSM.Map (LSMMap)

-- | A key-value store based on an LSM
type UnboxedVanillaMap k v = LSMMap Vector k (Last v)

-- | Make a map with only one value
singleton :: (Ord k, Unbox k, Unbox v) => k -> v -> UnboxedVanillaMap k v
singleton k v = LSM.singleton $ KV k (Last v)

-- | Add a key-value pair to the map
put :: (Ord k, Unbox k, Unbox v) => k -> v -> UnboxedVanillaMap k v -> UnboxedVanillaMap k v
put k v = LSM.push $ KV k (Last v)

-- | Get a key-value-pair from the map
get :: (Ord k, Unbox k, Unbox v) => k -> UnboxedVanillaMap k v -> Maybe v
-- I really hate using undefined here, wondering what other options I have
get k m = let
  err = error "Data.LSM.Vanilla: Requested the value of a query item, but there is no value in a query."
  in getLast . value <$> LSM.pull (KV k err) m

-- | Create a map from a list of entries
fromList :: (Ord k, Unbox k, Unbox v) => [(k, v)] -> UnboxedVanillaMap k v
fromList = LSM.fromList . fmap (\(k, v) -> KV k (Last v))

-- | Create a list of entries from a map
toList :: (Ord k, Unbox k, Unbox v) => UnboxedVanillaMap k v -> [(k, v)]
toList = fmap (\(KV k (Last v))->(k, v)) . LSM.toList
