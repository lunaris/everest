{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Everest.Kafka where

import qualified Everest as E

import qualified Conduit as Cdt
import qualified Control.Lens as Lens
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader)
import qualified Data.ByteString as BS
import Data.Coerce (coerce)
import Data.Functor (void)
import Data.Int (Int32)
import qualified Data.Generics.Product.Typed as G.P
import Data.Proxy (Proxy)
import Data.Foldable (for_)
import GHC.Generics (Generic)
import qualified Kafka.Consumer as K.C
import qualified Kafka.Producer as K.P

newtype ProducerT tag m a
  = ProducerT (m a)
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader r)

data ProducerConfig tag
  = ProducerConfig
      { _pcProducer :: !K.P.KafkaProducer
      }
  deriving stock Generic

instance ( MonadIO m
         , MonadReader r m
         , G.P.HasType (ProducerConfig tag) r
         )
      => E.MonadProducibleEventStore tag (ProducerT tag m) where
  type ProducerKey tag (ProducerT tag m)
    = BS.ByteString
  type ProducerValue tag (ProducerT tag m)
    = BS.ByteString
  writeEventsP =
    kWriteEventsP
  {-# INLINE writeEventsP #-}

kWriteEventsP
  :: forall tag r m
   . ( MonadIO m
     , MonadReader r m
     , G.P.HasType (ProducerConfig tag) r
     )
  => Proxy tag
  -> [E.WriteRecord BS.ByteString BS.ByteString]
  -> ProducerT tag m ()
kWriteEventsP _ptag wrs = do
  cfg <- Lens.view (G.P.typed @(ProducerConfig tag))
  let prod = _pcProducer cfg
      toProducerRecord wr =
        K.P.ProducerRecord
          { K.P.prTopic     = coerce (E._wrTopic wr)
          , K.P.prPartition = K.P.UnassignedPartition
          , K.P.prKey       = Just (E._wrKey wr)
          , K.P.prValue     = Just (E._wrValue wr)
          }
  void $ K.P.produceMessageBatch prod (toProducerRecord <$> wrs)

newtype ConsumerT tag m a
  = ConsumerT (m a)
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader r)

data ConsumerConfig tag
  = ConsumerConfig
      { _ccConsumer :: !K.C.KafkaConsumer
      }
  deriving stock Generic

instance ( MonadIO m
         , MonadReader r m
         , G.P.HasType (ConsumerConfig tag) r
         )
      => E.MonadConsumableEventStore tag (ConsumerT tag m) where
  type ConsumerMonad tag (ConsumerT tag m)
    = m
  type ConsumerKey tag (ConsumerT tag m)
    = BS.ByteString
  type ConsumerValue tag (ConsumerT tag m)
    = BS.ByteString
  allEventsP =
    kAllEventsP
  {-# INLINE allEventsP #-}

kAllEventsP
  :: forall tag r m i
   . ( MonadIO m
     , MonadReader r m
     , G.P.HasType (ConsumerConfig tag) r
     )
  => Proxy tag
  -> Proxy (ConsumerT tag m)
  -> [E.Topic]
  -> Cdt.ConduitT i (E.ReadRecord BS.ByteString BS.ByteString) m ()
kAllEventsP _ptag _pm _topics = do
  cfg <- Lens.view (G.P.typed @(ConsumerConfig tag))
  let con = _ccConsumer cfg
      fromConsumerRecord cr =
        E.ReadRecord
          { E._rrTopic     = coerce (K.C.crTopic cr)
          , E._rrPartition =
              coerce @(Int -> Int32) @(K.C.PartitionId -> E.Partition)
                fromIntegral (K.C.crPartition cr)
          , E._rrOffset    = coerce (K.C.crOffset cr)
          , E._rrTimestamp = undefined
          , E._rrKey       = undefined
          , E._rrValue     = undefined
          }
  errsOrMsgs <- K.C.pollMessageBatch con (K.C.Timeout 100) (K.C.BatchSize 100)
  for_ errsOrMsgs $ \case
    Left err ->
      -- TODO errors
      error $ "Gah " <> show err
    Right msg ->
      Cdt.yield (fromConsumerRecord msg)
