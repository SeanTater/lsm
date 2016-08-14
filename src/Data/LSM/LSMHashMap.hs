{-|
  This module implements a fast, ordered, cache-friendly, persistent, functional
  in-memory log structured merge tree, which is appropriate for sets and maps
  that focus on writes rather than reads.
-}
{-# LANGUAGE TemplateHaskell, FlexibleInstances, NoMonomorphismRestriction #-}
module Data.LSM.LSMHashMap (
  -- * A traditional map based on LSM
  LSMHashMap,
  singleton,
  put,
  get,
  fromList,
  toList
) where
import Data.Semigroup
import Data.LSM (KV(..))
import qualified Data.LSM as LSM
import qualified Data.Vector as V
import Data.LSM.Map (LSMMap)
import Data.Function
import Data.Hashable

-- | A key-value store based on an LSM
type LSMHashMap k v = LSMMap V.Vector (Hashed k) v
data Hashed v = Hashed {hashedH :: !Int, hashedV :: !v} deriving (Show)
instance Ord a => Ord (Hashed a) where
   compare x y = case (compare `on` hashedH) x y of
     LT -> LT
     GT -> GT
     EQ -> (compare `on` hashedV) x y
instance Eq a => Eq (Hashed a) where
  a == b = ((==) `on` hashedH) a b && ((==) `on` hashedV) a b

mkhash :: Hashable a => a -> Hashed a
mkhash v = Hashed (hash v) v

-- | Make a map with only one value
singleton :: (Ord k, Hashable k, Semigroup v) => k -> v -> LSMHashMap k v
singleton k v = LSM.singleton $ KV (mkhash k) v

-- | Add a key-value pair to the map
put :: (Ord k, Hashable k, Semigroup v) => k -> v -> LSMHashMap k v -> LSMHashMap k v
put k v = LSM.push $ KV (mkhash k) v

-- | Get a key-value-pair from the map
get :: (Ord k, Hashable k, Semigroup v) => k -> LSMHashMap k v -> Maybe v
-- I really hate using undefined here, wondering what other options I have
get k m = let
  err = error "Data.LSM.LSMHash: Requested the value of a query item, but there is no value in a query."
  in value <$> LSM.pull (KV (mkhash k) err) m

-- | Create a map from a list of entries
fromList :: (Ord k, Hashable k, Semigroup v) => [(k, v)] -> LSMHashMap k v
fromList = LSM.fromList . fmap (\(k, v) -> KV (mkhash k) v)

-- | Create a list of entries from a map
toList :: (Ord k, Hashable k, Semigroup v) => LSMHashMap k v -> [(k, v)]
toList = fmap (\(KV (Hashed _ k) v)->(k, v)) . LSM.toList
