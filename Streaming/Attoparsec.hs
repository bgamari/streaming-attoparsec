{-# LANGUAGE ScopedTypeVariables #-}

module Streaming.Attoparsec
    ( parseMany
    , parse
    ) where

import Control.Monad.Trans.Class
import qualified Streaming as S
import qualified Streaming.Prelude as S
import qualified Data.ByteString.Streaming as B
import qualified Data.ByteString as BS
import qualified Data.Attoparsec.ByteString as A

data ParseError m a = ParseError (B.ByteString m a) [String] String

data Result m a = Fail (ParseError m a)
                | Done (B.ByteString m a)

parse :: forall m a r. (Monad m)
      => A.Parser a
      -> B.ByteString m r
      -> S.Stream (S.Of a) m (Result m r)
parse parser = go (A.parse parser)
  where
    go :: (BS.ByteString -> A.Result a) -> B.ByteString m r -> S.Stream (S.Of a) m (Result m r)
    go res bs = do
        mr <- lift $ B.nextChunk bs
        case mr of
          Left ret ->
              case res mempty of
                A.Fail rest ctxts err -> return $ Fail $ ParseError (B.chunk rest >> return ret) ctxts err
                A.Partial _cont       -> return $ Fail $ ParseError (return ret) [] "Unexpected end of input"
                A.Done rest r         -> S.yield r >> return (Done $ B.chunk rest >> return ret)

          Right (b, bs') ->
              case res b of
                A.Fail rest ctxts err -> return $ Fail $ ParseError (rest `B.consChunk` bs') ctxts err
                A.Partial cont        -> go cont bs'
                A.Done rest r         -> S.yield r >> return (Done $ rest `B.consChunk` bs')

parseMany :: forall m a r. (Monad m)
          => A.Parser a
          -> B.ByteString m r
          -> S.Stream (S.Of a) m (Either (ParseError m r) r)
parseMany parser = go
  where
    go :: B.ByteString m r -> S.Stream (S.Of a) m (Either (ParseError m r) r)
    go bs = do
        empty <- lift $ B.null_ bs
        if empty
          then Right <$> lift (B.effects bs)
          else do
            r <- parse parser bs
            case r of
              Fail err  -> return $ Left err
              Done rest -> go rest
