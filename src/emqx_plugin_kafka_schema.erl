-module(emqx_plugin_kafka_schema).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0
    , fields/1
    , desc/1
]).

-import(hoconsc, [enum/1]).

roots() -> [plugin_kafka].

fields(plugin_kafka) ->
    [
        {connection, ?HOCON(?R_REF(connection), #{desc => ?DESC("connect_timeout")})},
        {producer, ?HOCON(?R_REF(producer), #{desc => ?DESC("connect_timeout")})},
        {hooks, ?HOCON(?ARRAY(?R_REF(hook)),
            #{
                required => true,
                default => [],
                desc => ?DESC("hooks")
            })}
    ];
fields(connection) ->
    [
        {client_id, ?HOCON(string(),
            #{
                desc => ?DESC("client_id"),
                default => "client"
            })},
        {bootstrap_hosts, bootstrap_hosts()},
        {connect_timeout, ?HOCON(emqx_schema:timeout_duration_ms(),
            #{
                default => <<"5s">>,
                desc => ?DESC("connect_timeout")
            })},
        {client_id, ?HOCON(string(),
            #{
                default => <<"emqx_plugin_kafka_connection">>,
                desc => ?DESC("client_id")
            })},
        {connection_strategy, ?HOCON(enum([per_partition, per_broker]),
            #{
                default => per_partition,
                desc => ?DESC("connection_strategy")
            }
        )},
        {min_metadata_refresh_interval, ?HOCON(emqx_schema:timeout_duration_ms(),
            #{
                default => <<"5s">>,
                desc => ?DESC("min_metadata_refresh_interval")
            }
        )},
        {query_api_versions, ?HOCON(boolean(),
            #{
                default => true,
                desc => ?DESC("query_api_versions")
            })},
        {request_timeout, ?HOCON(emqx_schema:timeout_duration_ms(),
            #{
                default => <<"3s">>,
                desc => ?DESC("request_timeout")
            }
        )},
        {sasl, ?HOCON(?R_REF(sasl),
            #{
                desc => ?DESC("sasl")
            }
        )},
        {ssl, ?HOCON(?R_REF(ssl),
            #{
                desc => ?DESC("ssl")
            }
        )},
        {health_check_interval, ?HOCON(emqx_schema:timeout_duration_ms(),
            #{
                default => <<"32s">>,
                desc => ?DESC("health_check_interval")
            }
        )}
    ];
fields(bootstrap_host) ->
    [
        {host, ?HOCON(string(),
            #{
                validator => emqx_schema:servers_validator(
                    #{default_port => 9092}, _Required = true)
            }
        )}
    ];
fields(sasl) ->
    [
        {mechanism, ?HOCON(enum([plain, scram_sha_256, scram_sha_512]),
            #{
                default => plain,
                desc => ?DESC("sasl_mechanism"),
                required => true
            }
        )},
        {username, ?HOCON(string(),
            #{
                desc => ?DESC("sasl_username"),
                required => true
            }
        )},
        {password, ?HOCON(string(),
            #{
                desc => ?DESC("sasl_password"),
                required => true
            }
        )}
    ];
fields(ssl) ->
    Schema = emqx_schema:client_ssl_opts_schema(#{}),
    lists:keydelete("user_lookup_fun", 1, Schema);
fields(producer) ->
    [
        {max_batch_bytes, ?HOCON(emqx_schema:bytesize(),
            #{
                default => "896KB",
                desc => ?DESC("max_batch_bytes")
            }
        )},
        {compression, ?HOCON(enum([no_compression, snappy, gzip]),
            #{
                default => no_compression,
                desc => ?DESC("compression")
            }
        )},
        {partition_strategy, ?HOCON(enum([random, roundrobin, first_key_dispatch]),
            #{
                default => random,
                desc => ?DESC("partition_strategy")
            }
        )},
        {encode_payload_type, ?HOCON(enum([plain, base64]),
            #{
                default => plain,
                desc => ?DESC("encode_payload_type")
            }
        )}
    ];
fields(hook) ->
    [
        {endpoint, ?HOCON(string(),
            #{
                desc => ?DESC("hook_endpoint"),
                required => true,
                validator => fun validate_endpoint/1
            })},
        {filter, ?HOCON(binary(),
            #{
                desc => ?DESC("hook_filter"),
                default => <<"#">>
            })},
        {kafka_topic, ?HOCON(string(),
            #{
                desc => ?DESC("hook_kafka_topic"),
                default => "emqx_test"
            })},
        {kafka_message, ?HOCON(?R_REF(kafka_message),
            #{
                desc => ?DESC("hook_kafka_message")
            })}
    ];
fields(kafka_message) ->
    [
        {key, ?HOCON(string(),
            #{
                default => <<"${.clientid}">>,
                desc => ?DESC(kafka_message_key)
            })},
        {value, ?HOCON(string(),
            #{
                default => <<"${.}">>,
                desc => ?DESC(kafka_message_value)
            })},
        {timestamp, ?HOCON(string(),
            #{
                default => <<"${.timestamp}">>,
                desc => ?DESC(kafka_message_timestamp)
            })}
    ].

desc(_) -> undefined.

bootstrap_hosts() ->
    Meta = #{desc => ?DESC("bootstrap_hosts")},
    emqx_schema:servers_sc(Meta, #{default_port => 9092}).

validate_endpoint(undefined) ->
    {error, "no matching mount point was found"};
validate_endpoint(Endpoint0) when is_list(Endpoint0) ->
    case emqx_utils:safe_to_existing_atom(Endpoint0) of
        {ok, Endpoint} ->
            validate_endpoint(emqx_plugin_kafka_hook:endpoint_func(Endpoint));
        _ ->
            {error, "no matching mount point was found"}
    end;
validate_endpoint(_) ->
    ok.
