{-# LANGUAGE OverloadedStrings, FlexibleContexts, ScopedTypeVariables #-}
import Prelude hiding (readFile, words)
import Criterion.Main
import Data.ByteString (ByteString, pack)
import Data.ByteString.Lazy (readFile, toStrict)
import Data.Hashable (Hashable)
import System.Random.MWC
import Codec.Compression.GZip (decompress)
import Data.Text.Encoding (decodeUtf8)
import Data.Semigroup

import qualified Data.Text as T
import qualified Data.CritBit.Map.Lazy as C
import qualified Data.HashMap.Strict as H
import qualified Data.IntMap as I
import qualified Data.Map as M
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Intro as I
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Unboxed as U
import qualified Data.LSM.Vanilla as Vanilla
import qualified Data.LSM.UnboxedVanilla as UnboxedVanilla
import qualified Data.LSM.LSMHashMap as LSMHashMap

type Vi = U.Vector Int
type Vb = V.Vector ByteString
type Vt = V.Vector T.Text

vboxSort :: Ord x => V.Vector x -> (V.Vector x, V.Vector x, V.Vector x)
vboxSort orig = let
  random    = G.take 100000 orig
  sorted    = G.modify I.sort random
  revsorted = G.reverse sorted
  in (random, sorted, revsorted)

tokens_uniform :: IO (Vt, Vt, Vt)
tokens_uniform = do
  -- Read and decompress some text from disk
  text <- decodeUtf8 . toStrict . decompress <$> readFile "kjv.verses.txt.gz"
  return $ vboxSort $ V.fromList $ T.chunksOf 5 text

tokens_nonuniform :: IO (Vt, Vt, Vt)
tokens_nonuniform = do
  -- Read and decompress some text from disk
  text <- decodeUtf8 . toStrict . decompress <$> readFile "kjv.verses.txt.gz"
  return $ vboxSort $ V.fromList $ T.words text

strings :: IO (Vb, Vb, Vb)
strings = do
  random <- withSystemRandom . asGenIO $ \gen ->
    V.replicateM 10000 $
      (pack . U.toList) `fmap` (uniformVector gen =<< uniformR (1,16) gen)
  return $ vboxSort random

numbers :: IO (Vi, Vi, Vi)
numbers = do
 random <- withSystemRandom . asGenIO $ \gen -> uniformVector gen 400000
 let sorted    = G.modify I.sort random
     revsorted = G.reverse sorted
 return (random, sorted, revsorted)

 -- Benchmark the cost of creating various types of map.
main :: IO ()
main = defaultMain [
        env numbers $ \ ~(random,sorted,revsorted) ->
        bgroup "Int" [
          bgroup "IntMap" [
            bench "sorted"    $ whnf intmap sorted
          , bench "random"    $ whnf intmap random
          , bench "revsorted" $ whnf intmap revsorted
          ]{-
        , bgroup "Map" [
            bench "sorted"    $ whnf mmap sorted
          , bench "random"    $ whnf mmap random
          , bench "revsorted" $ whnf mmap revsorted
          ]
        , bgroup "HashMap" [
            bench "sorted"    $ whnf hashmap sorted
          , bench "random"    $ whnf hashmap random
          , bench "revsorted" $ whnf hashmap revsorted
          ]-}
        , bgroup "LSM Unboxed" [
            bench "sorted"    $ whnf lsmunbox sorted
          , bench "random"    $ whnf lsmunbox random
          , bench "revsorted" $ whnf lsmunbox revsorted
          ]
        , bgroup "LSM Vanilla" [
            bench "sorted"    $ whnf lsm sorted
          , bench "random"    $ whnf lsm random
          , bench "revsorted" $ whnf lsm revsorted
          ]
        ]
      , {-env strings $ boxedTest "ByteString"
      , env tokens_uniform $ boxedTest "5-char slices of KJV"
      , -}env tokens_nonuniform $ boxedTest "Words of KJV"
      ]

boxedTest name = \ ~(random,sorted,revsorted) ->
  bgroup name [
    {-bgroup "Map" [
      bench "sorted"    $ whnf mmap sorted
    , bench "random"    $ whnf mmap random
    , bench "revsorted" $ whnf mmap revsorted
    ]
  , -}bgroup "HashMap" [
      bench "sorted"    $ whnf hashmap sorted
    , bench "random"    $ whnf hashmap random
    , bench "revsorted" $ whnf hashmap revsorted
    ]
  , {-bgroup "CritBit" [
      bench "sorted"    $ whnf critbit sorted
    , bench "random"    $ whnf critbit random
    , bench "revsorted" $ whnf critbit revsorted
    ]
  , bgroup "LSM Hash" [
      bench "sorted"    $ whnf lsmhm sorted
    , bench "random"    $ whnf lsmhm random
    , bench "revsorted" $ whnf lsmhm revsorted
    ]
  ,  -}bgroup "LSM Vanilla" [
      bench "sorted"    $ whnf lsm sorted
    , bench "random"    $ whnf lsm random
    , bench "revsorted" $ whnf lsm revsorted
    ]
  ]

critbit :: (G.Vector v k, C.CritBitKey k) => v k -> C.CritBit k Int
critbit xs = G.foldl' (\m k -> value `seq` C.insert k value m) C.empty xs

hashmap :: (G.Vector v k, Hashable k, Eq k) => v k -> H.HashMap k Int
hashmap xs = G.foldl' (\m k -> H.insert k value m) H.empty xs

intmap :: G.Vector v Int => v Int -> I.IntMap Int
intmap xs = G.foldl' (\m k -> I.insert k value m) I.empty xs

mmap :: (G.Vector v k, Ord k) => v k -> M.Map k Int
mmap xs = G.foldl' (\m k -> M.insert k value m) M.empty xs

lsmunbox :: (G.Vector v k, Ord k, U.Unbox k) => v k -> UnboxedVanilla.UnboxedVanillaMap k Int
lsmunbox xs = G.foldl' (\m k -> UnboxedVanilla.put k value m) mempty xs

lsm :: (G.Vector v k, Ord k) => v k -> Vanilla.VanillaMap k Int
lsm xs = G.foldl' (\m k -> Vanilla.put k value m) mempty xs
--lsm = Vanilla.fromListAsc . fmap (\k -> (k, value)) . G.toList

lsmhm :: (G.Vector v k, Ord k, Hashable k) => v k -> LSMHashMap.LSMHashMap k (Last Int)
lsmhm xs = G.foldl' (\m k -> LSMHashMap.put k (Last value) m) mempty xs

value :: Int
value = 31337
