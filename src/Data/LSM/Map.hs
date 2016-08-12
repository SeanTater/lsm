{-|
  An LSM makes a good choice as a write-heavy map using an associative operator.
  The difference between an LSM and an LSMMap is merely to store the keys along
  with the values. Entries are collated by key, and then the first key is kept,
  but the values are merged as a semigroup.

  If you prefer to always overwrite the previous value, wrap your value in
-}
module Data.LSM.Map (
  LSMMap,
  KV(..),
  singleton,
  put,
  fromList,
  toList
) where
import Data.LSM (LSM, Elem, ListLayer, VecLayer)
import qualified Data.LSM as LSM
import Data.Semigroup
import Data.Function

data KV k v = KV { key :: !k, value :: !v} deriving Show
instance Eq k => Eq (KV k v) where
  (==) = (==) `on` key                   -- Eq is on the keys
instance Ord k => Ord (KV k v) where
  compare = compare `on` key             -- Ord is on the keys
instance Semigroup v => Semigroup (KV k v) where
  (KV k a) <> (KV _ b) = KV k $ a <> b    -- But Semigroup is on the values
instance (Ord k, Semigroup v) => Elem (KV k v) where

-- | A key-value store based on an LSM
type LSMMap k v = LSM VecLayer (KV k v)

-- | Make a map with only one value
singleton :: (Ord k, Semigroup v) => k -> v -> LSMMap k v
singleton k v = LSM.singleton $ KV k v

-- | Add a key-value pair to the map
put :: (Ord k, Semigroup v) => k -> v -> LSMMap k v -> LSMMap k v
put k v = LSM.push (KV k v)

-- | Get a key-value-pair from the map
get :: (Ord k, Semigroup v) => k -> LSMMap k v -> Maybe v
-- I really hate using undefined here, wondering what other options I have
get k m = let
  err = error "Data.LSM.Map: Requested the value of a query item, but there is no value in a query."
  in value <$> LSM.pull (KV k err) m

-- | Create a LSM from an unordered list
fromList :: (Ord k, Semigroup v) => [(k, v)] -> LSMMap k v
fromList = LSM.fromList . fmap (\(k, v) -> KV k v)

-- | Dump an LSM to a maybe-unordered list. At the least, there will be long
-- ascending runs. After flatten, the full result will be strictly ascending.
toList :: (Ord k, Semigroup v) => LSMMap k v -> [(k, v)]
toList = fmap (\(KV k v) -> (k, v)) . LSM.toList
