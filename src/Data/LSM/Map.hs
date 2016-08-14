{-|
  This module is for convenience. It is the same as:
-}
{-# LANGUAGE FlexibleContexts, TypeFamilies, MultiParamTypeClasses, TemplateHaskell, FlexibleInstances, RankNTypes #-}
module Data.LSM.Map (
  LSMMap,
  singleton,
  get,
  put,
  fromList,
  toList
) where
import Data.LSM (LSM, VecLayer, KV(..))
import qualified Data.LSM as LSM
import qualified Data.Vector.Generic as G
import Data.Semigroup (Semigroup)

-- | A key-value store based on an LSM
--type LSMMap back k v = LSM (VecLayer back) (KV k v)
type LSMMap back k v = LSM (VecLayer back) (KV k v)

-- | Make a map with only one value
singleton :: (Ord k, Semigroup v, G.Vector back (KV k v)) => k -> v -> LSMMap back k v
singleton k v = LSM.singleton $ KV k v

-- | Add a key-value pair to the map
put :: (Ord k, Semigroup v, G.Vector back (KV k v)) => k -> v -> LSMMap back k v -> LSMMap back k v
put k v = LSM.push (KV k v)

-- | Get a key-value-pair from the map
get :: (Ord k, Semigroup v, G.Vector back (KV k v)) => k -> LSMMap back k v -> Maybe v
-- I really hate using undefined here, wondering what other options I have
get k m = let
  err = error "Data.LSM.Map: Requested the value of a query item, but there is no value in a query."
  in value <$> LSM.pull (KV k err) m

-- | Create a LSM from an unordered list
fromList :: (Ord k, Semigroup v, G.Vector back (KV k v)) => [(k, v)] -> LSMMap back k v
fromList = LSM.fromList . fmap (\(k, v) -> KV k v)

-- | Dump an LSM to a maybe-unordered list. At the least, there will be long
-- ascending runs. After flatten, the full result will be strictly ascending.
toList :: (Ord k, Semigroup v, G.Vector back (KV k v)) => LSMMap back k v -> [(k, v)]
toList = fmap (\(KV k v) -> (k, v)) . LSM.toList
