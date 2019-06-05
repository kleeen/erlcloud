%% Amazon textract Service

-module(erlcloud_textract).

%%% Library initialization.
-export([configure/2, configure/3, configure/4,  new/2, new/3]).

-export([analyze_document/2]).
-export([analyze_document_async/2]).
-export([analyze_document_async_status/1]).

-include_lib("erlcloud/include/erlcloud.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-spec new(string(), string()) -> aws_config().

new(AccessKeyID, SecretAccessKey) ->
    #aws_config{
       access_key_id=AccessKeyID,
       secret_access_key=SecretAccessKey
      }.

-spec new(string(), string(), string()) -> aws_config().

new(AccessKeyID, SecretAccessKey, Host) ->
    #aws_config{
       access_key_id=AccessKeyID,
       secret_access_key=SecretAccessKey,
       textract_host=Host
      }.


-spec new(string(), string(), string(), non_neg_integer()) -> aws_config().

new(AccessKeyID, SecretAccessKey, Host, Port) ->
    #aws_config{
       access_key_id=AccessKeyID,
       secret_access_key=SecretAccessKey,
       textract_host=Host,
       textract_port=Port
      }.

-spec configure(string(), string()) -> ok.

configure(AccessKeyID, SecretAccessKey) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey)),
    ok.

-spec configure(string(), string(), string()) -> ok.

configure(AccessKeyID, SecretAccessKey, Host) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey, Host)),
    ok.

-spec configure(string(), string(), string(), non_neg_integer()) -> ok.

configure(AccessKeyID, SecretAccessKey, Host, Port) ->
    put(aws_config, new(AccessKeyID, SecretAccessKey, Host, Port)),
    ok.

default_config() -> erlcloud_aws:default_config().

-spec analyze_document/2 :: (binary(), binary()) -> proplist().

analyze_document(Bucket, Filename) ->
   Json = [{<<"FeatureTypes">>, [<<"FORMS">>]}, {<<"Document">>, [{<<"S3Object">>, [{<<"Bucket">>, Bucket}, {<<"Name">>, Filename}]}]}],
   erlcloud_textract_impl:request(default_config(), "Textract.AnalyzeDocument", Json).

-spec analyze_document_async/2 :: (binary(), binary()) -> proplist().

analyze_document_async(Bucket, Filename) ->
   Json = [{<<"FeatureTypes">>, [<<"FORMS">>]}, {<<"DocumentLocation">>, [{<<"S3Object">>, [{<<"Bucket">>, Bucket}, {<<"Name">>, Filename}]}]}],
   erlcloud_textract_impl:request(default_config(), "Textract.StartDocumentTextDetection", Json).

-spec analyze_document_async_status/1 :: (binary()) -> proplist().

analyze_document_async_status(JobId) ->
   Json = [{<<"JobId">>, JobId}],
   erlcloud_textract_impl:request(default_config(), "Textract.GetDocumentTextDetection", Json).
