{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Everest.PostgreSQL
  ( EventStoreT (..)
  , EventStoreConfig (..)
  ) where

import qualified Everest as E

import qualified Conduit as Cdt
import qualified Control.Lens as Lens
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader)
import Control.Monad.Trans.Class (lift)
import qualified Data.Aeson as Ae
import Data.Functor (void)
import qualified Data.Generics.Product.Typed as G.P
import Data.Proxy (Proxy)
import qualified Data.Text as Tx
import qualified Data.UUID as U
import qualified Database.PostgreSQL.Simple as PG
import qualified Database.PostgreSQL.Simple.Types as PG.Types
import GHC.Generics (Generic)

newtype EventStoreT tag m a
  = EventStoreT (m a)
  deriving newtype (Applicative, Functor, Monad,
                    MonadIO, MonadReader r)

data EventStoreConfig tag
  = EventStoreConfig
      { _escConnection :: !PG.Connection
      , _escTable      :: !PG.Types.QualifiedIdentifier
      }
  deriving (Generic)

instance ( MonadIO m
         , MonadReader r m
         , G.P.HasType (EventStoreConfig tag) r
         )
      => E.MonadEventStore tag (EventStoreT tag m) where
  type Key tag (EventStoreT tag m)
    = U.UUID
  type Value tag (EventStoreT tag m)
    = Ae.Value
  writeEvents' =
    pgWriteEvents'
  {-# INLINE writeEvents' #-}

pgWriteEvents'
  :: forall tag r m
   . ( MonadIO m
     , MonadReader r m
     , G.P.HasType (EventStoreConfig tag) r
     )
  => Proxy tag
  -> [E.WriteRecord U.UUID Ae.Value]
  -> EventStoreT tag m ()
pgWriteEvents' _prx wrs = do
  cfg <- Lens.view (G.P.typed @(EventStoreConfig tag))
  let conn = _escConnection cfg
      tbl  = _escTable cfg
  void $ liftIO $ PG.execute conn
    "insert into ? (type, aggregate_id, data) ? on conflict do nothing"
    ( tbl
    , PG.Types.Values ["text", "uuid", "jsonb"]
        [(ty, k, v) | E.WriteRecord ty k v <- wrs]
    )
  pure ()

data ProjectingEventStoreConfig tag
  = ProjectingEventStoreConfig
      { _pescConnection :: !PG.Connection
      , _pescTable      :: !PG.Types.QualifiedIdentifier
      , _pescChannel    :: !Tx.Text
      , _pescOffset     :: !E.Offset
      }
  deriving (Generic)

instance ( MonadIO m
         , MonadReader r m
         , G.P.HasType (ProjectingEventStoreConfig tag) r
         )
      => E.MonadProjectingEventStore tag (EventStoreT tag m) where
  allEvents' =
    pgAllEvents'
  {-# INLINE allEvents' #-}

pgAllEvents'
  :: forall tag r m i
   . ( MonadIO m
     , MonadReader r m
     , G.P.HasType (ProjectingEventStoreConfig tag) r
     )
  => Proxy tag
  -> Cdt.ConduitT
      i
      (E.ReadRecord (E.Key tag (EventStoreT tag m)) (E.Value tag (EventStoreT tag m)))
      (EventStoreT tag m)
      ()
pgAllEvents' _prx = do
  (conn, chan) <- lift $ do
    cfg <- Lens.view (G.P.typed @(ProjectingEventStoreConfig tag))
    let conn = _pescConnection cfg
        chan = _pescChannel cfg
    void $ liftIO $ PG.execute conn "listen ?" (PG.Only chan)
    pure (conn, chan)
  undefined
