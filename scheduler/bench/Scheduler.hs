module Main where

import Control.Concurrent.Async (mapConcurrently, mapConcurrently_)
import Control.Monad.Par (parMapM, runParIO)
import Control.Parallel (par)
import Control.Scheduler (Comp(Par), traverseConcurrently, traverseConcurrently_)
import Criterion.Main
import Data.Foldable as F
import UnliftIO.Async (pooledMapConcurrently, pooledMapConcurrently_)


main :: IO ()
main = defaultMain (mkSumBench 1000)

mkSumBench :: Int -> [Benchmark]
mkSumBench n =
  [ env (pure ls) $ \xs ->
      bgroup
        ("Sums: " <> show n)
        [ bgroup "unliftio" [bench "pooledMapConcurrently" $ nfIO (pooledMapConcurrently f xs)]
        , bgroup "monad-par" [bench "parMapM" $ nfIO (runParIO $ parMapM f xs)]
        , bgroup "scheduler" [bench "traverseConcurrently" $ nfIO (traverseConcurrently Par f xs)]
        , bgroup "async" [bench "mapConcurrently" $ nfIO (mapConcurrently f xs)]
        , bgroup "base" [bench "traverse . par" $ nfIO (traverse fpar xs)]
        , bgroup "base" [bench "traverse . seq" $ nfIO (traverse f xs)]
        ]
  ]
  where
    ls = replicate n [0 .. 100000] :: [[Int]]
    f xs =
      let ys = F.foldl' (+) 0 xs
       in ys `seq` pure ys
    fpar xs =
      let ys = F.foldl' (+) 0 xs
       in ys `par` pure ys
