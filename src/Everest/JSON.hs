module Everest.JSON where

import qualified Everest as E

import qualified Control.Lens as Lens
import qualified Data.Aeson as Ae
import qualified Data.Aeson.Lens as Ae.Lens

writeRecord
  :: ( Ae.ToJSON a
     , Ae.Lens.AsValue v
     )
  => E.Topic
  -> k
  -> a
  -> E.WriteRecord k v
writeRecord topic k v =
  E.WriteRecord
    { E._wrTopic = topic
    , E._wrKey   = k
    , E._wrValue = Lens.review Ae.Lens._Value (Ae.toJSON v)
    }
