-module(emqx_plugin_kafka_util).

-include_lib("emqx/include/logger.hrl").

-export([
    resource_id/0
    , channel_id/1
    , check_config/2
]).

-export([
    check_crc32cer_nif/0
]).

resource_id() ->
    <<"emqx_plugin:kafka_producer">>.

client_id(ClientId) ->
    <<"emqx_plugin:kafka_client:", (bin(ClientId))/binary>>.

channel_id(Endpoint) ->
    <<"emqx_plugin:kafka_producer:connector:", (bin(Endpoint))/binary>>.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

check_config(Key, Config) when is_map_key(Key, Config) ->
    tr_config(Key, maps:get(Key, Config));
check_config(Key, _Config) ->
    throw(#{
        reason => missing_required_config,
        missing_config => Key
    }).

tr_config(bootstrap_hosts, Hosts) ->
    hosts(Hosts);
tr_config(client_id, ClientId) ->
    client_id(ClientId);
tr_config(sasl, Sasl) ->
    sasl(Sasl);
tr_config(ssl, Ssl) ->
    ssl(Ssl);
tr_config(_Key, Value) ->
    Value.

%% Parse comma separated host:port list into a [{Host,Port}] list
hosts(Hosts) when is_binary(Hosts) ->
    hosts(binary_to_list(Hosts));
hosts([#{hostname := _, port := _} | _] = Servers) ->
    [{Hostname, Port} || #{hostname := Hostname, port := Port} <- Servers];
hosts(Hosts) when is_list(Hosts) ->
    kpro:parse_endpoints(Hosts).

sasl(#{mechanism := Mechanism, username := Username, password := Secret}) ->
    {Mechanism, Username, Secret};
sasl(_) ->
    undefined.

ssl(#{enable := true} = SSL) ->
    emqx_tls_lib:to_client_opts(SSL);
ssl(_) ->
    false.

check_crc32cer_nif() ->
    try
        crc32cer:nif("1")
    catch
        error:{crc32cer_nif_not_loaded, _}:_ ->
            load_crc32cer_nif()
    end.

load_crc32cer_nif() ->
    code:purge(crc32cer),
    code:load_file(crc32cer).