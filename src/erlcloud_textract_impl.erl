-module(erlcloud_textract_impl).

-include("erlcloud.hrl").
-include("erlcloud_aws.hrl").

-export([backoff/1, retry/2]).
-export([request/3]).

-type json_return() :: {ok, jsx:json_term()} | {error, term()}.

-type operation() :: string().
-spec request(aws_config(), operation(), jsx:json_term()) -> json_return().
request(Config0, Operation, Json) ->
    Body = case Json of
               [] -> <<"{}">>;
               _ -> jsx:encode(Json)
           end,
    case erlcloud_aws:update_config(Config0) of
        {ok, Config} ->
            Headers = headers(Config, Operation, Body),
            request_and_retry(Config, Headers, Body, {attempt, 1});
        {error, Reason} ->
            {error, Reason}
    end.

%% Error handling
%% see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ErrorHandling.html

%% Although it is documented that you should use exponential backoff, exact delays or number of retries
%% are not specified.
%% boto (if I read the code correctly) waits 2^(Attempt - 2)*50ms before an attempt and will make 10 attempts,
%% which means it will wait up to 12.8 seconds before the last attempt.
%% This algorithm is similar, except that it waits a random interval up to 2^(Attempt-2)*100ms. The average
%% wait time should be the same as boto.

%% TODO make delay configurable
%% TODO refactor retry logic so that it can be used by all requests and move to erlcloud_aws

-define(NUM_ATTEMPTS, 10).

%% Sleep after an attempt
-spec backoff(pos_integer()) -> ok.
backoff(1) -> ok;
backoff(Attempt) ->
    timer:sleep(random:uniform((1 bsl (Attempt - 1)) * 100)).

-type attempt() :: {attempt, pos_integer()} | {error, term()}.
-type retry_fun() :: fun((pos_integer(), term()) -> attempt()).
-spec retry(pos_integer(), term()) -> attempt().
retry(Attempt, Reason) when Attempt >= ?NUM_ATTEMPTS ->
    {error, Reason};
retry(Attempt, _) ->
    backoff(Attempt),
    {attempt, Attempt + 1}.

-type headers() :: [{string(), string()}].
-spec request_and_retry(aws_config(), headers(), jsx:json_text(), attempt()) ->
                               {ok, jsx:json_term()} | {error, term()}.
request_and_retry(_, _, _, {error, Reason}) ->
    {error, Reason};
request_and_retry(Config, Headers, Body, {attempt, Attempt}) ->
    RetryFun = Config#aws_config.textract_retry,
    case erlcloud_httpc:request(
           url(Config), post,
           [{<<"content-type">>, <<"application/x-amz-json-1.1">>} | Headers],
           Body, erlcloud_aws:get_timeout(Config), Config) of

        {ok, {{200, _}, _, RespBody}} ->
            %% TODO check crc
            {ok, jsx:decode(RespBody)};

        {ok, {{Status, StatusLine}, _, RespBody}} when Status >= 400 andalso Status < 500 ->
            case client_error(Status, StatusLine, RespBody) of
                {retry, Reason} ->
                    request_and_retry(Config, Headers, Body, RetryFun(Attempt, Reason));
                {error, Reason} ->
                    {error, Reason}
            end;

        {ok, {{Status, StatusLine}, _, RespBody}} when Status >= 500 ->
            request_and_retry(Config, Headers, Body, RetryFun(Attempt, {http_error, Status, StatusLine, RespBody}));

        {ok, {{Status, StatusLine}, _, RespBody}} ->
            {error, {http_error, Status, StatusLine, RespBody}};

        {error, Reason} ->
            %% TODO there may be some http errors, such as certificate error, that we don't want to retry
            request_and_retry(Config, Headers, Body, RetryFun(Attempt, Reason))
    end.

-spec client_error(pos_integer(), string(), binary()) -> {retry, term()} | {error, term()}.
client_error(Status, StatusLine, Body) ->
    case jsx:is_json(Body) of
        false ->
            {error, {http_error, Status, StatusLine, Body}};
        true ->
            Json = jsx:decode(Body),
            case proplists:get_value(<<"__type">>, Json) of
                undefined ->
                    {error, {http_error, Status, StatusLine, Body}};
                FullType ->
                    Message = proplists:get_value(<<"message">>, Json, <<>>),
                    case binary:split(FullType, <<"#">>) of
                        [_, <<"ProvisionedThroughputExceededException">> = Type] ->
                            {retry, {Type, Message}};
                        [_, <<"ThrottlingException">> = Type] ->
                            {retry, {Type, Message}};
                        [_, Type] ->
                            {error, {Type, Message}};
                        _ ->
                            {error, {http_error, Status, StatusLine, Body}}
                    end
            end
    end.

-spec headers(aws_config(), string(), binary()) -> headers().
headers(Config, Operation, Body) ->
    Headers = [{"host", Config#aws_config.textract_host},
               {"x-amz-target", Operation}],
    erlcloud_aws:sign_v4_headers(Config, Headers, Body, erlcloud_aws:aws_region_from_host(Config#aws_config.textract_host), "textract").

url(#aws_config{textract_scheme = Scheme, textract_host = Host} = Config) ->
    lists:flatten([Scheme, Host, port_spec(Config)]).

port_spec(#aws_config{textract_port=80}) ->
    "";
port_spec(#aws_config{textract_port=Port}) ->
    [":", erlang:integer_to_list(Port)].
