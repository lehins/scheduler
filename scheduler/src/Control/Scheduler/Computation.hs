{-# OPTIONS_HADDOCK hide, not-home #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- |
-- Module      : Control.Scheduler.Computation
-- Copyright   : (c) Alexey Kuleshevich 2018-2021
-- License     : BSD3
-- Maintainer  : Alexey Kuleshevich <lehins@yandex.ru>
-- Stability   : experimental
-- Portability : non-portable
--
module Control.Scheduler.Computation
  ( Comp(.., Par, Par'), compNumWorkers, getCompWorkers
  ) where

import Primal.Eval (NFData(..), deepseq)
import Primal.Concurrent (getNumCapabilities)
import Primal.Monad.Unsafe (unsafePerformIO)
import Data.Word

-- | Computation strategy to use when scheduling work.
data Comp
  = Seq -- ^ Sequential computation
  | ParOn ![Int]
  -- ^ Schedule workers to run on specific capabilities. Specifying an empty list @`ParOn` []@ or
  -- using `Par` will result in utilization of all available capabilities.
  | ParN {-# UNPACK #-} !Word16
  -- ^ Specify the number of workers that will be handling all the jobs. Difference from `ParOn` is
  -- that workers can jump between cores. Using @`ParN` 0@ will result in using all available
  -- capabilities.
  deriving Eq

-- | Parallel computation using all available cores. Same as @`ParOn` []@
--
-- @since 1.0.0
pattern Par :: Comp
pattern Par <- ParOn [] where
        Par =  ParOn []

-- | Parallel computation using all available cores. Same as @`ParN` 0@
--
-- @since 1.1.0
pattern Par' :: Comp
pattern Par' <- ParN 0 where
        Par' =  ParN 0

instance Show Comp where
  show Seq        = "Seq"
  show Par        = "Par"
  show (ParOn ws) = "ParOn " ++ show ws
  show (ParN n)   = "ParN " ++ show n
  showsPrec _ Seq  = ("Seq" ++)
  showsPrec _ Par  = ("Par" ++)
  showsPrec 0 comp = (show comp ++)
  showsPrec _ comp = (("(" ++ show comp ++ ")") ++)

instance NFData Comp where
  rnf comp =
    case comp of
      Seq        -> ()
      ParOn wIds -> wIds `deepseq` ()
      ParN n     -> n `deepseq` ()
  {-# INLINE rnf #-}

instance Monoid Comp where
  mempty = Seq
  {-# INLINE mempty #-}
  mappend = joinComp
  {-# INLINE mappend #-}

instance Semigroup Comp where
  (<>) = joinComp
  {-# INLINE (<>) #-}

joinComp :: Comp -> Comp -> Comp
joinComp x y =
  case x of
    Seq -> y
    Par -> Par
    Par' -> Par'
    ParOn xs ->
      case y of
        Par      -> Par
        Par'     -> Par'
        ParOn ys -> ParOn (xs <> ys)
        _        -> x
    ParN n1 ->
      case y of
        Seq     -> x
        Par     -> Par
        ParOn _ -> y
        Par'    -> y
        ParN n2 -> ParN (max n1 n2)
{-# NOINLINE joinComp #-}

numCaps :: Int
numCaps = unsafePerformIO getNumCapabilities
{-# NOINLINE numCaps #-}

-- | Figure out how many workers will this computation strategy create.
--
-- /Note/ - If at any point during program execution global number of capabilities gets
-- changed with `Control.Concurrent.setNumCapabilities`, it will have no affect on this
-- function, unless it hasn't yet been called with `Par` or `Par'` arguments. In other
-- words, it is a pure value and it will not change throughout execution of the program.
--
-- @since 2.0.0
compNumWorkers :: Comp -> Int
compNumWorkers =
  \case
    Seq      -> 1
    Par      -> numCaps
    ParOn ws -> length ws
    Par'     -> numCaps
    ParN n   -> fromIntegral n


getCompWorkers :: Monad m => Comp -> m Int
getCompWorkers = pure . compNumWorkers
{-# DEPRCATED getCompWorkers "In favor of pure `compNumWorkers`" #-}
