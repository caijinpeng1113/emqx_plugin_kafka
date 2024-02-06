-module(emqx_plugin_kafka_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2
    , stop/1
]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_plugin_kafka_sup:start_link(),
    emqx_plugin_kafka:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_plugin_kafka:unload(),
    ok.