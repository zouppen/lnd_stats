{-# LANGUAGE OverloadedStrings, DeriveGeneric, DeriveAnyClass #-}

module Main where

import Control.Applicative
import Data.Aeson
import Data.Int
import Data.Text (Text)
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow
import GHC.Generics
import System.Posix.Env.ByteString (getArgs)
import Text.Read (readMaybe)

sqlFindNew = "SELECT run_id, o FROM cmd_json i WHERE cmd='listchannels' AND NOT EXISTS (SELECT FROM channel o WHERE i.run_id = o.run_id)"
sqlInsertNew = "INSERT INTO channel VALUES (?,?,?,?,?,?,?,?,?)"

-- |Uses read instance of a type to parse the value. Sometimes useful
-- to dig numbers from a string type.
mRead :: (Alternative m, Read a) => String -> m a 
mRead = maybe empty pure . readMaybe

data Channel = Channel { chanId   :: Text
                       , remote   :: Text
                       , private  :: Bool
                       , active   :: Bool
                       , capacity :: Integer
                       , local    :: Integer
                       , uptime   :: Text
                       , lifetime :: Text
                       } deriving (Show, Generic, ToRow)

instance FromField Channel where
  fromField = fromJSONField

instance FromJSON Channel where
  parseJSON = withObject "Channel" $ \o -> do
    local <- do
      myCommitFee <- do
        my <- o .: "initiator"
        if my
          then o .: "commit_fee" >>= mRead
          else pure 0
      localRaw <- o .: "local_balance" >>= mRead
      pendings <- o .: "pending_htlcs"
      pure $ sum $ myCommitFee : localRaw : map pending pendings
    Channel
      <$> o .: "chan_id"
      <*> o .: "remote_pubkey"
      <*> o .: "private"
      <*> o .: "active"
      <*> (o .: "capacity" >>= mRead)
      <*> pure local
      <*> o .: "uptime"
      <*> o .: "lifetime"

-- Local pending amount
newtype Pending = Pending { pending :: Integer }

instance FromJSON Pending where
  parseJSON = withObject "Pending" $ \o -> do
    amount <- o .: "amount" >>= mRead
    incoming <- o .: "incoming"
    pure $ Pending $ if incoming
                     then 0
                     else amount

main = do
  [dbConnStr] <- getArgs
  conn <- connectPostgreSQL dbConnStr
  materialize conn >>= print

materialize :: Connection -> IO Int64
materialize conn = withTransaction conn $ do
  xs <- query_ conn sqlFindNew
  executeMany conn sqlInsertNew $ map mankeloi xs

mankeloi :: (Int, Channel) -> [Action]
mankeloi (rowId, chan) = toField rowId : toRow chan
