{-|
  This module implements a fast, ordered, cache-friendly, persistent, functional
  in-memory log structured merge tree, which is appropriate for sets and maps
  that focus on writes rather than reads. This implementation is mostly notable
  for batched merges between matching entries, which that can avoid
  read-modify-write cycles, and hopefully reduce latency and increase throughput
  by improving memory locality and reducing garbage.
-}
{-# LANGUAGE TemplateHaskell, FlexibleInstances, NoMonomorphismRestriction #-}
{-# LANGUAGE MultiParamTypeClasses, RecordWildCards, BangPatterns, RankNTypes #-}
module Data.LSM (
  -- * Creating and Using LSMs as Sets
  LSM,
  Elem,
  Layer,
  ListLayer,
  VecLayer,
  singleton,
  push,
  pull,
  flatten,
  fromList,
  toList,
) where
import Data.Semigroup
import Data.Function
import Data.Maybe
import Data.List (foldl', find)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import qualified Data.Vector.Fusion.Bundle as Bun
import qualified Data.Vector.Algorithms.Merge as VMerge
import Control.Applicative
import Data.Traversable
-- | An element in the set
class (Ord e, Semigroup e) => Elem e where

-- | One layer of an LSM: It must:
--
--   * store elements in sorted order
--   * retrieve a given element by Eq, optionally find it with Ord
--   * have a cheaply computed length
--   * merge consecutive elements using Semigroup or Monoid
--   * convert to and from a list
class (Monoid (l e), Semigroup (l e)) => Layer l e where
  -- | Return the length of a layer
  len       :: Elem e => l e -> Int
  single    :: Elem e => e -> l e
  -- | Create a layer from a list
  toLayer   :: Elem e => [e] -> l e
  -- | Create a list from a layer
  fromLayer :: Elem e => l e -> [e]
  -- | Retrieve an equal element
  fetch     :: Elem e => e -> l e -> Maybe e


-- | One layer in the table, an ordered List with precomputed length
data ListLayer e = ListLayer {
  llen :: !Int,
  items :: ![e]
} deriving (Show, Eq)
instance Elem e => Layer ListLayer e where
  len = llen
  single = ListLayer 1 . (:[])
  toLayer l = ListLayer (length l) l
  fromLayer = items
  fetch a = find (==a) . fromLayer
instance Elem e => Monoid (ListLayer e) where
  mempty = toLayer []
  mappend = mergeS `on` fromLayer
instance Elem e => Semigroup (ListLayer e) where
-- | Merge consecutive non-increasing entries with a semigroup.
--   *NOTE! It is assumed that the list is in reverse order, since prepending is
--   faster. So the semigroup merges occur in reverse order again to flip it
--   back.*
--
--   It's not clear to me if it would be better to use == rather than >= as a
--   criterion for merging.

uniqRaw :: Elem e => [e] -> [e]
uniqRaw = fmap fromJust . filter isJust . snd . mapAccumL merge_some Nothing
merge_some :: Elem a => Maybe a -> a -> (Maybe a, Maybe a)
merge_some Nothing this = (Just this, Nothing)
merge_some (Just prev) this = if prev == this
  then (Nothing, Just $ this<>prev)
  else (Just this, Just prev)

-- |  The 'merge' function combines all elements of two ordered lists.
-- An element occurs in the output as many times as the sum of the
-- occurrences in both lists.   The output is a set if and only if
-- the inputs are disjoint sets.
--
-- > merge [ 1,2, 3,4 ] [ 3,4, 5,6 ]   == [ 1,2,  3,3,4,4,  5,6 ]
-- > merge [ 1, 2,2,2 ] [ 1,1,1, 2,2 ] == [ 1,1,1,1,  2,2,2,2,2 ]
mergeS :: Elem a => [a] -> [a] -> ListLayer a
mergeS a b = ListLayer (length merged) merged
  where
    merged = loop a b
    loop [] ys = ys
    loop xs [] = xs
    loop (x:xs) (y:ys) = case compare x y of
      GT -> y : loop (x:xs) ys
      EQ -> (y <> x) : loop xs ys
      LT -> x : loop xs (y:ys)

mergeV :: Elem a => [a] -> [a] -> [a]
mergeV [] ys = ys
mergeV xs [] = xs
mergeV (x:xs) (y:ys) = case compare x y of
  GT -> y : mergeV (x:xs) ys
  EQ -> (y <> x) : mergeV xs ys
  LT -> x : mergeV xs (y:ys)

mergeVS :: Elem a => V.Vector a -> V.Vector a -> V.Vector a
mergeVS vx vy = V.create $ do
  let xl = V.length vx
  let yl = V.length vy

  vo <- MV.unsafeNew (xl+yl)
  let loop io ix iy =
        if ix < xl && iy < yl
        then do
          x <- V.unsafeIndexM vx ix
          y <- V.unsafeIndexM vy iy
          case compare x y of
            GT -> (MV.unsafeWrite vo io $! y)      >> loop (io+1) ix     (iy+1)
            EQ -> (MV.unsafeWrite vo io $! (y<>x)) >> loop (io+1) (ix+1) (iy+1)
            LT -> (MV.unsafeWrite vo io $! x)      >> loop (io+1) (ix+1) iy
        else
          return io
  (\i -> MV.take i vo) <$> loop 0 0 0


-- | One layer in the table, an ordered boxed Vector
newtype VecLayer a = VecLayer {unvec :: V.Vector a} deriving (Show, Eq)
instance Elem e => Layer VecLayer e where
  len = V.length . unvec
  single = VecLayer . V.singleton
  toLayer = VecLayer . V.fromList
  fromLayer = V.toList . unvec
  fetch a = V.find (==a) . unvec
instance Elem e => Monoid (VecLayer e) where
  mempty = toLayer []
  --mappend a b = toLayer $ uniqRaw $ OrdList.merge (fromLayer a) (fromLayer b)
  mappend a b = VecLayer $ mergeVS (unvec a) (unvec b)
  --mappend a b = uniq $ VecLayer $ V.modify (VMerge.sort) $ mappend (unvec a) (unvec b) -- vector ++ is concat

instance Elem e => Semigroup (VecLayer e) where

{-| A fast, lazy table useful for nonuniform access patterns.
  It can be used directly as a Set; consider LSMMap for a Map overlay.
  Writes are much faster than reads in an LSM, because they happen lazily,
  they require no read-modify-write cycles, and for nonuniform access, the
  most common keys will be in the first layers so cache will be used
  efficiently.

  This implementation of a Log Structured Merge tree is a little unorthodox
  on two accounts.

  * It uses plain linked lists, so reads are slow O(n) but writes are fast
    O(log n) amortized, worst case O(n), with low constant factors.

  * You can use any associative operator to lazily merge elements. This allows
    you to not only replace the previous value, but also to accumulate values.
    It's analogous to HashMap's insertWith, and fromListWith, but since this
    map is lazy, the operator must be consistent. Hence the requirement that
    the records are in Ord and Semigroup, rather than allowing you to specify
    an operator.

  @
    -- Example words
    let w = words "ocelot ocelot elk deer ocelot"
    -- Counting using HashMap
    HashMap.fromListWith (+) $ (\x -> (x, 1)) <$> w
    -- Counting using an existing HashMap
    HashMap.insertWith (+) ((\x -> (x, 1)) <$> w) existing_hashmap
    -- Counting using a LSM requires types (for your own sanity)
    LSM.fromList $ one <$> w
  @
-}
newtype LSM l e = LSM {unstack :: [l e]} deriving (Show)
instance (Layer l e, Elem e) => Monoid (LSM l e) where
  -- | Make an empty table
  mempty = LSM []
  -- | Merge two tables
  mappend a b = clean $ LSM $ ((<>) `on` unstack) a b
instance (Layer l e, Elem e) => Semigroup (LSM l e) where

-- | Make a table with one entry
singleton :: (Layer l e, Elem e) => e -> LSM l e
singleton a = LSM [single a]

-- | Add a record to a table
push :: (Layer l e, Elem e) => e -> LSM l e -> LSM l e
push item (LSM []) = LSM [single item]
push item (LSM (s:ss))= clean $ LSM (single item <> s : ss)

-- | Find a record in the table, or Nothing.
pull :: (Layer l e, Elem e) => e -> LSM l e -> Maybe e
pull a lsm = foldr (<|>) Nothing $ fetch a <$> unstack lsm

--   Clean up the table.
--   The layers should have ascending lengths, if they
--   do not, this will merge them until that is true.
clean :: (Layer l e, Elem e) => LSM l e -> LSM l e
clean = LSM . foldr org [] . unstack
  where
    org x [] = x : []
    org x (y:ys) = if (len x) >= (len y)
      then (x <> y) : ys
      else (x:y:ys)

-- | Flatten an LSM into a single ordered layer. A subsequent toList will be
-- ascending, and any subsequent reads will be faster. It's O(n) and should be
-- put off until after most of the writing is complete.
flatten :: (Layer l e, Elem e) => LSM l e -> LSM l e
flatten = LSM . (:[]) . mconcat . unstack

-- | Create a LSM from an unordered list
fromList :: (Layer l e, Elem e) => [e] -> LSM l e
fromList = foldl' (flip push) mempty

-- | O(n log n) Dump an LSM to a ordered list. This might be expensive.
toList :: (Layer l e, Elem e) => LSM l e -> [e]
toList = fromLayer . fromMaybe mempty . find (const True) . unstack . flatten
