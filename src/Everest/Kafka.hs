{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Everest.Kafka where

import qualified Everest as E

import qualified Control.Lens as Lens
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader)
import qualified Data.ByteString as BS
import Data.Coerce (coerce)
import Data.Functor (void)
import qualified Data.Generics.Product.Typed as G.P
import Data.Proxy (Proxy)
import GHC.Generics (Generic)
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
  --  TODO: This or produceMessageBatch?
  void $ traverse (K.P.produceMessage prod . toProducerRecord) wrs
