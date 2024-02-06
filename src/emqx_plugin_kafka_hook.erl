-module(emqx_plugin_kafka_hook).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_plugin_kafka.hrl").

-export([
    hooks/3
    , unhook/0
]).

-export([
    endpoint_func/1
]).

-export([
    on_client_connect/3
    , on_client_connack/4
    , on_client_connected/3
    , on_client_disconnected/4
    , on_client_authenticate/3
    , on_client_authorize/5
    , on_client_check_authz_complete/6
]).

-export([
    on_session_created/3
    , on_session_subscribed/4
    , on_session_unsubscribed/4
    , on_session_resumed/3
    , on_session_discarded/3
    , on_session_takenover/3
    , on_session_terminated/4
]).

-export([
    on_message_publish/2
    , on_message_delivered/3
    , on_message_acked/3
    , on_message_dropped/4
]).

-define(evt_mod, emqx_plugin_kafka_evt).

hooks([Hook | T], Producer, Acc) ->
    Ret = hook(emqx_plugin_kafka_util:resource_id(), Hook#{producer => Producer}),
    hooks(T, Producer, [Ret | Acc]);
hooks([], _, Acc) ->
    persistent_term:put({?EMQX_PLUGIN_KAFKA_APP, ?EMQX_PLUGIN_KAFKA_CHANNELS}, Acc).

hook(ResId, Hook = #{endpoint := Endpoint0, filter := Filter}) ->
    {ok, Endpoint} = emqx_utils:safe_to_existing_atom(Endpoint0),
    ChannelId = emqx_plugin_kafka_util:channel_id(Endpoint),
    emqx_resource_manager:add_channel(ResId, ChannelId, Hook),
    Opts = #{
        channel_id => ChannelId,
        filter => Filter
    },
    trigger_hook(Endpoint, endpoint_func(Endpoint), Opts),
    {ChannelId, Hook}.

trigger_hook(_, undefined, _) ->
    ok;
trigger_hook(Endpoint, Func, Opts) ->
    emqx_hooks:add(Endpoint, {?MODULE, Func, [Opts]}, _Property = ?HP_HIGHEST).

endpoint_func('client.connect') -> on_client_connect;
endpoint_func('client.connack') -> on_client_connack;
endpoint_func('client.connected') -> on_client_connected;
endpoint_func('client.disconnected') -> on_client_disconnected;
endpoint_func('client.authenticate') -> on_client_authenticate;
endpoint_func('client.authorize') -> on_client_authorize;
endpoint_func('client.authenticate') -> on_client_authenticate;
endpoint_func('client.check_authz_complete') -> on_client_check_authz_complete;
endpoint_func('session.created') -> on_session_created;
endpoint_func('session.subscribed') -> on_session_subscribed;
endpoint_func('session.unsubscribed') -> on_session_unsubscribed;
endpoint_func('session.resumed') -> on_session_resumed;
endpoint_func('session.discarded') -> on_session_discarded;
endpoint_func('session.takenover') -> on_session_takenover;
endpoint_func('session.terminated') -> on_session_terminated;
endpoint_func('message.publish') -> on_message_publish;
endpoint_func('message.delivered') -> on_message_delivered;
endpoint_func('message.acked') -> on_message_acked;
endpoint_func('message.dropped') -> on_message_dropped;
endpoint_func(_) -> undefined.

unhook() ->
    unhook('client.connect', {?MODULE, on_client_connect}),
    unhook('client.connack', {?MODULE, on_client_connack}),
    unhook('client.connected', {?MODULE, on_client_connected}),
    unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    unhook('client.authenticate', {?MODULE, on_client_authenticate}),
    unhook('client.authorize', {?MODULE, on_client_authorize}),
    unhook('client.check_authz_complete', {?MODULE, on_client_check_authz_complete}),
    unhook('session.created', {?MODULE, on_session_created}),
    unhook('session.subscribed', {?MODULE, on_session_subscribed}),
    unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    unhook('session.resumed', {?MODULE, on_session_resumed}),
    unhook('session.discarded', {?MODULE, on_session_discarded}),
    unhook('session.takenover', {?MODULE, on_session_takenover}),
    unhook('session.terminated', {?MODULE, on_session_terminated}),
    unhook('message.publish', {?MODULE, on_message_publish}),
    unhook('message.delivered', {?MODULE, on_message_delivered}),
    unhook('message.acked', {?MODULE, on_message_acked}),
    unhook('message.dropped', {?MODULE, on_message_dropped}).

unhook(Endpoint, MFA) ->
    emqx_hooks:del(Endpoint, MFA).

%%--------------------------------------------------------------------
%% Client Lifecycle Hooks
%%--------------------------------------------------------------------

on_client_connect(ConnInfo, Props, Opts) ->
    query(?evt_mod:eventmsg_connect(ConnInfo), Opts),
    {ok, Props}.

on_client_connack(ConnInfo, Rc, Props, Opts) ->
    query(?evt_mod:eventmsg_connack(ConnInfo, Rc), Opts),
    {ok, Props}.

on_client_connected(ClientInfo, ConnInfo, Opts) ->
    query(?evt_mod:eventmsg_connected(ClientInfo, ConnInfo), Opts),
    ok.

on_client_disconnected(ClientInfo, ReasonCode, ConnInfo, Opts) ->
    query(?evt_mod:eventmsg_disconnected(ClientInfo, ConnInfo, ReasonCode), Opts),
    ok.

on_client_authenticate(ClientInfo, Result, Opts) ->
    query(?evt_mod:eventmsg_authenticate(ClientInfo, Result), Opts),
    {ok, Result}.

on_client_authorize(ClientInfo, PubSub, Topic, Result, Opts) ->
    query(?evt_mod:eventmsg_authorize(ClientInfo, PubSub, Topic, Result), Opts),
    {ok, Result}.

on_client_check_authz_complete(ClientInfo, PubSub, Topic, Result, AuthzSource, Opts) ->
    query(?evt_mod:eventmsg_check_authz_complete(ClientInfo, PubSub, Topic, Result, AuthzSource), Opts),
    {ok, Result}.

%%--------------------------------------------------------------------
%% Session Lifecycle Hooks
%%--------------------------------------------------------------------

on_session_created(ClientInfo, #{id := SessionId, created_at := CreatedAt}, Opts) ->
    query(?evt_mod:eventmsg_session_created(ClientInfo, SessionId, CreatedAt), Opts),
    ok.

on_session_subscribed(ClientInfo, Topic, SubOpts, Opts) ->
    query(?evt_mod:eventmsg_sub_or_unsub('session.subscribed', ClientInfo, Topic, SubOpts), Opts),
    ok.

on_session_unsubscribed(ClientInfo, Topic, SubOpts, Opts) ->
    query(?evt_mod:eventmsg_sub_or_unsub('session.unsubscribed', ClientInfo, Topic, SubOpts), Opts),
    ok.

on_session_resumed(ClientInfo, _SessInfo, Opts) ->
    query(?evt_mod:eventmsg_session('session.resumed', ClientInfo), Opts),
    ok.

on_session_discarded(ClientInfo, _SessInfo, Opts) ->
    query(?evt_mod:eventmsg_session('session.discarded', ClientInfo), Opts),
    ok.

on_session_takenover(ClientInfo, _SessInfo, Opts) ->
    query(?evt_mod:eventmsg_session('session.takenover', ClientInfo), Opts),
    ok.

on_session_terminated(ClientInfo, Reason, _SessInfo, Opts) ->
    query(?evt_mod:eventmsg_session_terminated(ClientInfo, Reason), Opts),
    ok.

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

on_message_publish(Message, Opts = #{filter := Filter}) ->
    case match_topic(Message, Filter) of
        true ->
            query(?evt_mod:eventmsg_publish(Message), Opts);
        false ->
            ok
    end,
    {ok, Message}.

on_message_dropped(Message, #{node := ByNode}, Reason, Opts = #{filter := Filter}) ->
    case match_topic(Message, Filter) of
        true ->
            query(?evt_mod:eventmsg_dropped(Message, ByNode, Reason), Opts);
        false ->
            ok
    end,
    ok.

on_message_delivered(ClientInfo, Message, Opts = #{filter := Filter}) ->
    case match_topic(Message, Filter) of
        true ->
            query(?evt_mod:eventmsg_delivered(ClientInfo, Message), Opts);
        false ->
            ok
    end,
    {ok, Message}.

on_message_acked(ClientInfo, Message, Opts = #{filter := Filter}) ->
    case match_topic(Message, Filter) of
        true ->
            query(?evt_mod:eventmsg_acked(ClientInfo, Message), Opts);
        false ->
            ok
    end,
    ok.

%%%===================================================================
%%% External functions
%%%===================================================================

match_topic(_, <<$#, _/binary>>) ->
    false;
match_topic(_, <<$+, _/binary>>) ->
    false;
match_topic(#message{topic = <<"$SYS/", _/binary>>}, _) ->
    false;
match_topic(#message{topic = Topic}, Filter) ->
    emqx_topic:match(Topic, Filter);
match_topic(_, _) ->
    false.

query(
    EvtMsg,
    #{channel_id := ChannelId}
) ->
    query_ret(
        emqx_resource:query(emqx_plugin_kafka_util:resource_id(), {ChannelId, EvtMsg}),
        EvtMsg
    ).

query_ret({_, {ok, _}}, _) ->
    ok;
query_ret(Ret, EvtMsg) ->
    ?SLOG(error,
        #{
            msg => "failed_to_query_kafka_resource",
            ret => Ret,
            evt_msg => EvtMsg
        }).