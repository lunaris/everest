{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

{-# OPTIONS_GHC -Wnot #-}

module Everest.Kafka where

import qualified Everest as E

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader)

newtype ProducerT tag m a
  = ProducerT (m a)
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader r)

kWriteEventsP _ptag wrs = do
  pure ()
