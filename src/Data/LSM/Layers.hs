{-# LANGUAGE TemplateHaskell, FlexibleInstances, NoMonomorphismRestriction #-}
{-# LANGUAGE MultiParamTypeClasses, RecordWildCards, BangPatterns, RankNTypes, TypeFamilies #-}
module Data.LSM.Layers (
  -- * LSM Legos
  -- | You can build LSM with KV to make a map, use the First and Last wrappers
  --   to manage keeping/overwriting existing records, etc.
  Layer(..),
  ListLayer,
  VecLayer,
  Elem
) where
import Data.Semigroup hiding (First(..), Last(..))
import Data.Function
import Data.List (foldl', find)
import qualified Data.Vector.Generic as V
import qualified Data.Vector.Generic.Mutable as MV
import qualified Data.Vector.Algorithms.Merge as VMerge
import Control.Applicative

-- For the Legos
import Data.Vector.Unboxed.Deriving

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
  -- | Make an layer of just one item
  single    :: Elem e => e -> l e
  single = toLayer . (:[])
  -- | Create a layer from a list
  toLayer   :: Elem e => [e] -> l e
  -- | Create a list from a layer
  fromLayer :: Elem e => l e -> [e]
  -- | Retrieve an equal element
  fetch     :: Elem e => e -> l e -> Maybe e


-- | An LSM layer built on a singly linked list.
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
instance Elem e => Semigroup (ListLayer e)
instance Elem e => Monoid (ListLayer e) where
  mempty = toLayer []
  mappend = mergeL `on` fromLayer where
    -- | Merge consecutive non-increasing entries with a semigroup.
    --   *NOTE! It is assumed that the list is in reverse order, since prepending is
    --   faster. So the semigroup merges occur in reverse order again to flip it
    --   back.*
    --
    --   It's not clear to me if it would be better to use == rather than >= as a
    --   criterion for merging.
    mergeL :: Elem a => [a] -> [a] -> ListLayer a
    mergeL a b = ListLayer (length merged) merged
      where
        merged = loop a b
        loop [] ys = ys
        loop xs [] = xs
        loop (x:xs) (y:ys) = case compare x y of
          GT -> y : loop (x:xs) ys
          EQ -> (y <> x) : loop xs ys
          LT -> x : loop xs (y:ys)

-- | An LSM layer built on a generic Vector. It can be either boxed or unboxed.
--   In all cases it is immutable, referentially transparent, and persistent.
newtype VecLayer v a = VecLayer {unvec :: v a}
instance (V.Vector v e, Elem e) => Layer (VecLayer v) e where
  len = V.length . unvec
  single = VecLayer . V.singleton
  toLayer = VecLayer . V.fromList
  fromLayer = V.toList . unvec
  fetch a = V.find (==a) . unvec
instance (V.Vector v e, Elem e) => Semigroup (VecLayer v e)
instance (V.Vector v e, Elem e) => Monoid (VecLayer v e) where
  mempty = toLayer []
  mappend = mergeV `on` unvec where
    -- | Merge consecutive non-increasing entries with a semigroup. See mergeLL.
    --   Differs in that it runs on a mutable vector internally.
    mergeV :: (V.Vector v a, Elem a) => v a -> v a -> VecLayer v a
    mergeV vx vy = VecLayer $ V.create $ do
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
