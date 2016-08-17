{-|
  A lazy, ordered table or set, useful for nonuniform access patterns, on account
  of reasonable locality of reference. Whether LSM is the fastest choice depends
  on when you are reading this and your access patterns. However, compared to
  other persistent sets, LSMs are no slouch and can be fast for write-heavy
  workloads. The highlights of LSMs are:

  * An LSM is natively an ordered set. In this case it is not a multiset: there
    may be only one copy of an item per LSM. (But you are free to make that item
    a list; in fact this is simple and efficient.)

  * Read-modify-write cycles are done lazily, which is why the items must be in
    a Semigroup: some merges may happen a long time later in batches. Hence the
    typical word-count map and similar use patterns should be fast since there
    is no immediate fetch: only a deferred modification. It's still not magic:
    it is similar to mergesort will be O(n log2 n) in the long run.

  * Vectors (especially unboxed vectors) have predictable memory layout and good
    locality of reference so the resulting maps should be somewhat compact, and
    make good use of cache.

  * LSM's based on linked lists have O(n) retrieval times, which even for small
    n, may be pretty slow. LSM's based on vectors have O(log^2 n) retrieval
    times: worse than most ordered sets and much worse than Hashmap.
    You can manage this by using flatten: afterward it has O(log n) retrieval
    time. If you can guarantee no more edits then you can get better constants
    for gargantuan sets, but that's not implemented yet.

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
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE BangPatterns           #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TypeFamilies           #-}
module Data.LSM (
  -- * Creating and Using LSMs
  LSM,
  Elem,
  singleton,
  push,
  pull,
  flatten,
  fromList,
  fromListAsc,
  toList,
  -- * LSM Legos
  -- | You can build LSM with KV to make a map, use the First and Last wrappers
  --   to manage keeping/overwriting existing records, etc.
  First(..),
  Last(..),
  KV(..)
) where
import Data.Semigroup hiding (First(..), Last(..))
import Data.Function
import Data.Maybe
import Data.List (foldl', find)
import Data.LSM.Layers
import Control.Applicative

-- For the Legos
import Data.Vector.Unboxed.Deriving
import Data.Vector.Unboxed (Unbox)


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
push item (LSM (s:ss)) = clean . LSM . (: ss) $! (single item <> s)

-- | Find a record in the table. Records are compared with Eq and Ord so:
--
--  * For simple types like Bool or Int, this is really only useful for finding
--    if the item is present, which is easily done:
--
--    > Maybe.isJust $ LSM.pull 1828 the_map
--
--  * For more complex types (like KV, or anything else you like) you can make
--    Eq only compare some of the fields: the others could be different. For KV,
--    the value is not used in Eq, so the value may be interesting.
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

-- | Flatten an LSM into a single ordered layer. A flattened LSM has lower
--   complexity and faster reads, and the same complexity and speed writes.
--   But after writing, a flat LSM is no longer flat, and flattening an LSM
--   is O(n log n) and possibly quite slow. It's recommended to flatten only
--   before long read-only periods.
flatten :: (Layer l e, Elem e) => LSM l e -> LSM l e
flatten = LSM . (:[]) . mconcat . unstack

-- | Create an LSM from an unordered list incrementally.
fromList :: (Layer l e, Elem e) => [e] -> LSM l e
fromList = foldl' (flip push) mempty

-- | Create an LSM from an unordered list, as a batch. Small batches are not
--   harshly penalized and it should be less than 1/2 of available memory, and
--   preferably a good deal less. There's no guarantee this is faster, so check.
fromListAsc :: (Layer l e, Elem e) => [e] -> LSM l e
fromListAsc batch = LSM . (:[]) $! toLayer batch

-- | O(n log n) Dump an LSM to a ordered list. This might be expensive.
toList :: (Layer l e, Elem e) => LSM l e -> [e]
toList = fromLayer . fromMaybe mempty . find (const True) . unstack . flatten


{-
--  Building blocks for making cooler maps
-}

-- | Use First to combine two records, only keeping the first one you inserted.
--   It means that once a key is set, it can not be modified.
newtype First a = First {getFirst :: a} deriving (Show, Eq, Ord)
instance Semigroup (First a) where (<>) = const
-- | Use Last to always overwrite the previous value of a record.
--   This is probably the way you are used to a map working.
newtype Last a = Last {getLast :: a} deriving (Show, Eq, Ord)
instance Semigroup (Last a) where (<>) = flip const

derivingUnbox "First"
    [t| forall a. (Unbox a) => First a -> a |]
    [| getFirst |]
    [| First |]
derivingUnbox "Last"
    [t| forall a. (Unbox a) => Last a -> a |]
    [| getLast |]
    [| Last |]


-- | Store keys and values in an LSM, sorting by the key and merging the values.
--   The keys aren't merged. The first is always kept.
--   If the key and value are both unboxable (as in Data.Vector.Unboxed) then so
--   is the corresponding KV.
data KV k v = KV { key :: !k, value :: !v} deriving Show
instance Eq k => Eq (KV k v) where
  (==) = (==) `on` key                   -- Eq is on the keys
instance Ord k => Ord (KV k v) where
  compare = compare `on` key             -- Ord is on the keys
instance Semigroup v => Semigroup (KV k v) where
  (KV k a) <> (KV _ b) = KV k $ a <> b    -- But Semigroup is on the values
instance (Ord k, Semigroup v) => Elem (KV k v) where

derivingUnbox "KV"
    [t| forall k v. (Unbox k, Unbox v) => KV k v -> (k, v) |]
    [| \(KV k v) -> (k, v) |]
    [| uncurry KV |]
