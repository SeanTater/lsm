{-|
  This module implements a fast, ordered, cache-friendly, persistent, functional
  in-memory log structured merge tree, which is appropriate for sets and maps
  that focus on writes rather than reads.
-}
module Data.LSM.Vanilla (
  -- * A traditional map based on LSM
  VanillaMap,
  singleton,
  put,
  get,
  fromList,
  fromListAsc,
  toList
) where
import Data.Semigroup
import Data.LSM (KV(..))
import qualified Data.LSM as LSM
import qualified Data.Vector as V
import Data.LSM.Map (LSMMap)
-- | A key-value store based on an LSM
type VanillaMap k v = LSMMap (V.Vector) k (Last v)

-- | Make a map with only one value
singleton :: (Ord k) => k -> v -> VanillaMap k v
singleton k v = LSM.singleton $ KV k (Last v)

-- | Add a key-value pair to the map
put :: (Ord k) => k -> v -> VanillaMap k v -> VanillaMap k v
put k v = LSM.push $ KV k (Last v)

-- | Get a key-value-pair from the map
get :: (Ord k, Semigroup v) => k -> VanillaMap k v -> Maybe v
-- I really hate using undefined here, wondering what other options I have
get k m = let
  err = error "Data.LSM.Vanilla: Requested the value of a query item, but there is no value in a query."
  in getLast . value <$> LSM.pull (KV k err) m

-- | Create a map from a list of entries
fromList :: (Ord k) => [(k, v)] -> VanillaMap k v
fromList = LSM.fromList . fmap (\(k, v) -> KV k (Last v))

fromListAsc :: (Ord k) => [(k, v)] -> VanillaMap k v
fromListAsc = LSM.fromListAsc . fmap (\(k, v) -> KV k (Last v))

-- | Create a list of entries from a map
toList :: (Ord k) => VanillaMap k v -> [(k, v)]
toList = fmap (\(KV k (Last v))->(k, v)) . LSM.toList
