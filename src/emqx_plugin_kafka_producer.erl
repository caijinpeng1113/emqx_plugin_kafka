-module(emqx_plugin_kafka_producer).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_plugin_kafka.hrl").

-export([
    query_mode/1
    , callback_mode/0
    , on_start/2
    , on_get_status/2
    , on_stop/2
    , on_add_channel/4
    , on_get_channels/1
    , on_get_channel_status/3
    , on_remove_channel/3
    , on_query_async/4
]).

query_mode(_) ->
    simple_async_internal_buffer.

callback_mode() ->
    async_if_possible.

on_start(
    _InstId,
    #{connection := Connection}
) ->
    C = fun(Key) -> emqx_plugin_kafka_util:check_config(Key, Connection) end,
    Hosts = C(bootstrap_hosts),
    ClientId = C(client_id),
    ClientConfig = #{
        connect_timeout => C(connect_timeout),
        connection_strategy => C(connection_strategy),
        min_metadata_refresh_interval => C(min_metadata_refresh_interval),
        query_api_versions => C(query_api_versions),
        request_timeout => C(request_timeout),
        sasl => C(sasl),
        ssl => C(ssl)
    },
    ok = ensure_client(ClientId, Hosts, ClientConfig),
    case check_client_connectivity(ClientId) of
        ok ->
            {ok, #{
                client_id => ClientId,
                channels => #{}
            }};
        {error, {find_client, Reason}} ->
            %% Race condition?  Crash?  We just checked it with `ensure_client'...
            {error, Reason};
        {error, {connectivity, Reason}} ->
            {error, Reason}
    end.

on_get_status(
    _InstId,
    #{client_id := ClientId} = State
) ->
    case check_client_connectivity(ClientId) of
        ok ->
            ?status_connected;
        {error, {find_client, _Error}} ->
            ?status_connecting;
        {error, {connectivity, Error}} ->
            {?status_connecting, State, Error}
    end.

on_stop(_InstId, #{client_id := ClientId, channels := Channels}) ->
    ?SLOG(info, #{
        msg => "kafka_client_on_stop",
        client_id => ClientId
    }),
    maps:foreach(fun(_, ChannelState) -> remove_producers(ClientId, ChannelState) end, Channels),
    deallocate_client(ClientId),
    persistent_term:erase({?EMQX_PLUGIN_KAFKA_APP, ?EMQX_PLUGIN_KAFKA_CHANNELS}),
    ok.

on_add_channel(
    InstId,
    #{
        client_id := ClientId,
        channels := Channels
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = start_producers(InstId, ChannelId, ClientId, ChannelConfig),
    NChannels = maps:put(ChannelId, ChannelState, Channels),
    NewState = OldState#{channels => NChannels},
    {ok, NewState}.

on_get_channels(_InstId) ->
    persistent_term:get({?EMQX_PLUGIN_KAFKA_APP, ?EMQX_PLUGIN_KAFKA_CHANNELS}, []).

on_get_channel_status(
    _InstId,
    _ChannelId,
    _State
) ->
    ?status_connected.

on_remove_channel(
    _InstId,
    #{
        client_id := ClientId,
        channels := Channels
    } = OldState,
    ChannelId
) ->
    case maps:take(ChannelId, Channels) of
        {ChannelState, NChannels} ->
            remove_producers(ClientId, ChannelState),
            NewState = OldState#{channels => NChannels},
            {ok, NewState};
        error ->
            {ok, OldState}
    end.

on_query_async(
    InstId,
    {ChannelId, Message},
    _,
    #{channels := Channels} = _ConnectorState
) ->
    #{
        message_template := Template,
        producers := Producers,
        encode_payload_type := EncodePayloadType
    } = maps:get(ChannelId, Channels),
    try
        KafkaMessage = render_message(Template, Message, EncodePayloadType),
        do_send_msg(KafkaMessage, Producers)
    catch
        Error:Reason :Stack ->
            ?SLOG(error, #{
                msg => "emqx_plugin_kafka_producer on_query_async error",
                error => Error,
                instId => InstId,
                reason => Reason,
                stack => Stack
            }),
            {error, {Error, Reason}}
    end.

%%%===================================================================
%%% External functions
%%%===================================================================

check_client_connectivity(ClientId) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            case wolff_client:check_connectivity(Pid) of
                ok ->
                    ok;
                {error, Error} ->
                    {error, {connectivity, Error}}
            end;
        {error, Reason} ->
            {error, {find_client, Reason}}
    end.

ensure_client(ClientId, Hosts, ClientConfig) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, _Pid} ->
            ok;
        {error, no_such_client} ->
            case wolff:ensure_supervised_client(ClientId, Hosts, ClientConfig) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => failed_to_start_kafka_client,
                        client_id => ClientId,
                        kafka_hosts => Hosts,
                        reason => Reason
                    }),
                    throw(failed_to_start_kafka_client)
            end;
        {error, Reason} ->
            deallocate_client(ClientId),
            throw({failed_to_find_created_client, Reason})
    end.

deallocate_client(ClientId) ->
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_client(ClientId) end,
        #{
            msg => "failed_to_delete_kafka_client",
            client_id => ClientId
        }
    ),
    ok.

start_producers(
    InstId,
    ChannelId,
    ClientId,
    #{
        kafka_topic := KafkaTopic,
        kafka_message := MessageTemplate,
        producer := Producer
    }
) ->
    #{encode_payload_type := EncodePayloadType} = Producer,
    WolffProducerConfig = producers_config(Producer),
    case wolff:ensure_supervised_producers(ClientId, KafkaTopic, WolffProducerConfig) of
        {ok, Producers} ->
            {ok, #{
                message_template => compile_message_template(MessageTemplate),
                kafka_topic => KafkaTopic,
                producers => Producers,
                encode_payload_type => EncodePayloadType
            }};
        {error, Reason2} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_producer",
                instance_id => InstId,
                channel_id => ChannelId,
                kafka_topic => KafkaTopic,
                reason => Reason2
            }),
            throw(
                "Failed to start Kafka client. Please check the logs for errors and check"
                " the connection parameters."
            )
    end.

producers_config(#{
    max_batch_bytes := MaxBatchBytes,
    compression := Compression,
    partition_strategy := PartitionStrategy
}) ->
    #{
        partitioner => PartitionStrategy,
        replayq_dir => false,
        replayq_offload_mode => false,
        max_batch_bytes => MaxBatchBytes,
        compression => Compression
    }.

remove_producers(ClientId, #{producers := Producers}) ->
    deallocate_producers(ClientId, Producers),
    ok.

deallocate_producers(ClientId, Producers) ->
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_producers(Producers) end,
        #{
            msg => "failed_to_delete_kafka_producer",
            client_id => ClientId
        }
    ).

compile_message_template(T) ->
    KeyTemplate = maps:get(key, T, <<"${.clientid}">>),
    ValueTemplate = maps:get(value, T, <<"${.}">>),
    TimestampTemplate = maps:get(timestamp, T, <<"${.timestamp}">>),
    #{
        key => preproc_tmpl(KeyTemplate),
        value => preproc_tmpl(ValueTemplate),
        timestamp => preproc_tmpl(TimestampTemplate)
    }.

preproc_tmpl(Tmpl) ->
    emqx_placeholder:preproc_tmpl(Tmpl).

render_message(
    #{key := KeyTemplate, value := ValueTemplate, timestamp := TimestampTemplate},
    Message,
    EncodePayloadType
) ->
    #{
        key => render(KeyTemplate, Message),
        value => encode_payload(EncodePayloadType, render(ValueTemplate, Message)),
        ts => render_timestamp(TimestampTemplate, Message)
    }.

render(Template, Message) ->
    Opts = #{
        var_trans => fun
                         (undefined) -> <<"">>;
                         (X) -> emqx_utils_conv:bin(X)
                     end,
        return => full_binary
    },
    emqx_placeholder:proc_tmpl(Template, Message, Opts).

render_timestamp(Template, Message) ->
    try
        binary_to_integer(render(Template, Message))
    catch
        _:_ ->
            erlang:system_time(millisecond)
    end.

encode_payload(base64, Payload) ->
    base64:encode(Payload);
encode_payload(_, Payload) ->
    Payload.

do_send_msg(KafkaMessage, Producers) ->
    {_Partition, Pid} = wolff:send(Producers, [KafkaMessage], fun(_Partition, _BaseOffset) -> ok end),
    {ok, Pid}.

with_log_at_error(Fun, Log) ->
    try
        Fun()
    catch
        C:E ->
            ?SLOG(error, Log#{
                exception => C,
                reason => E
            })
    end.