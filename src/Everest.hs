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
import qualified Data.Time as T

class MonadProducibleEventStore (tag :: k) (m :: Type -> Type) where
  type ProducerKey tag m
    :: Type
  type ProducerValue tag m
    :: Type
  writeEventsP
    :: Proxy tag
    -> [ProducerWriteRecord tag m]
    -> m ()

writeEvents
  :: forall tag m
   . MonadProducibleEventStore tag m
  => [ProducerWriteRecord tag m]
  -> m ()
writeEvents =
  writeEventsP (Proxy @tag)

class MonadConsumableEventStore (tag :: k) (m :: Type -> Type) where
  type ConsumerMonad tag m
    :: Type -> Type
  type ConsumerKey tag m
    :: Type
  type ConsumerValue tag m
    :: Type
  allEventsP
    :: Proxy tag
    -> Proxy m
    -> [Topic]
    -> Cdt.ConduitT i (ConsumerReadRecord tag m) (ConsumerMonad tag m) ()

allEvents
  :: forall tag m i
   . ( MonadConsumableEventStore tag m
     , m ~ ConsumerMonad tag m
     )
  => [Topic]
  -> Cdt.ConduitT i (ConsumerReadRecord tag m) m ()
allEvents =
  allEventsP (Proxy @tag) (Proxy @m)

data WriteRecord k v
  = WriteRecord
      { _wrTopic :: !Topic
      , _wrKey   :: !k
      , _wrValue :: !v
      }
  deriving (Eq, Functor, Generic, Ord, Show)

type ProducerWriteRecord tag m
  = WriteRecord (ProducerKey tag m) (ProducerValue tag m)

data ReadRecord k v
  = ReadRecord
      { _rrTopic     :: !Topic
      , _rrPartition :: !Partition
      , _rrOffset    :: !Offset
      , _rrTimestamp :: !T.UTCTime
      , _rrKey       :: !k
      , _rrValue     :: !v
      }
  deriving (Eq, Functor, Generic, Ord, Show)

type ConsumerReadRecord tag m
  = ReadRecord (ConsumerKey tag m) (ConsumerValue tag m)

newtype Topic
  = Topic { getTopic :: Tx.Text }
  deriving (Eq, Ord, Show)

newtype Partition
  = Partition { getPartition :: Int32 }
  deriving (Eq, Ord, Show)

newtype Offset
  = Offset { getOffset :: Int64 }
  deriving (Eq, Ord, Show)
