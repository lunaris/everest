{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module KafkaConsumer where

import Conduit ((.|))
import qualified Conduit as Cdt
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader, ReaderT (..))
import Data.Foldable (fold)
import qualified Everest as E
import qualified Everest.Kafka as E.K
import GHC.Generics (Generic)
import qualified Kafka.Consumer as K.C

--------------------------------------------------------------------------------
--  Example producer
--------------------------------------------------------------------------------

main :: IO ()
main = do
  let conProps = fold
        [ K.C.brokersList [K.C.BrokerAddress "localhost:9092"]
        , K.C.groupId (K.C.ConsumerGroupId "account-X")
        , K.C.noAutoCommit
        ]
      cc = E.K.ConsumerConfig
        { E.K._ccConsumerProperties = conProps
        , E.K._ccConsumerOffsetReset = K.C.Earliest
        , E.K._ccPollTimeout = K.C.Timeout 5000
        , E.K._ccPollBatchSize = K.C.BatchSize 100
        }
      env = Env
        { _eConsumerConfig = cc
        }
  runApp env $ Cdt.runConduit $
       E.allEvents @"store" [E.Topic "Account"]
    .| Cdt.mapM_C (liftIO . print)

newtype App a
  = App { _runApp :: ReaderT Env IO a }
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader Env)
  deriving (E.MonadConsumableEventStore "store")
    via (E.K.ConsumerT "store" App)

data Env
  = Env
      { _eConsumerConfig :: E.K.ConsumerConfig "store"
      }

  deriving stock Generic

runApp :: Env -> App a -> IO a
runApp env (App m)
  = runReaderT m env
