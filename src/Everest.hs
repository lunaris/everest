{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Everest where

import qualified Conduit as Cdt
import Data.Int (Int32, Int64)
import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import GHC.Generics (Generic)
import qualified Data.Text as Tx

class MonadEventStore (tag :: k) (m :: Type -> Type) where
  type Key tag m
    :: Type
  type Value tag m
    :: Type
  writeEvents'
    :: Proxy tag
    -> [WriteRecord (Key tag m) (Value tag m)]
    -> m ()

writeEvents
  :: forall tag m
   . MonadEventStore tag m
  => [WriteRecord (Key tag m) (Value tag m)]
  -> m ()
writeEvents =
  writeEvents' (Proxy @tag)

data WriteRecord k v
  = WriteRecord
      { _wrType  :: !Tx.Text
      , _wrKey   :: !k
      , _wrValue :: !v
      }
  deriving (Eq, Functor, Generic, Ord, Show)

class MonadEventStore tag m => MonadProjectingEventStore tag m where
  allEvents
    :: Cdt.ConduitT i (ReadRecord (Key tag m) (Value tag m)) m ()

data ReadRecord k v
  = ReadRecord
      { _rrType      :: !Tx.Text
      , _rrPartition :: !Partition
      , _rrOffset    :: !Offset
      , _rrKey       :: !k
      , _rrValue     :: !v
      }
  deriving (Eq, Functor, Generic, Ord, Show)

newtype Partition
  = Partition { getPartition :: Int32 }
  deriving (Eq, Ord, Show)

newtype Offset
  = Offset { getOffset :: Int64 }
  deriving (Eq, Ord, Show)
