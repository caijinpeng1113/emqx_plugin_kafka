-module(emqx_plugin_kafka_evt).

-include_lib("emqx/include/emqx.hrl").

-export([
    eventmsg_connect/1
    , eventmsg_connack/2
    , eventmsg_connected/2
    , eventmsg_disconnected/3
    , eventmsg_authenticate/2
    , eventmsg_authorize/4
    , eventmsg_check_authz_complete/5

    , eventmsg_session_created/3
    , eventmsg_sub_or_unsub/4
    , eventmsg_session/2
    , eventmsg_session_terminated/2

    , eventmsg_publish/1
    , eventmsg_dropped/3
    , eventmsg_delivered/2
    , eventmsg_acked/2
]).

eventmsg_publish(
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp
    }
) ->
    with_basic_columns(
        'message.publish',
        #{
            id => emqx_guid:to_hexstr(Id),
            clientid => ClientId,
            username => emqx_message:get_header(username, Message, undefined),
            payload => Payload,
            peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined)),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        }
    ).

eventmsg_connect(
    ConnInfo = #{
        peername := PeerName,
        sockname := SockName,
        clean_start := CleanStart,
        proto_name := ProtoName,
        proto_ver := ProtoVer
    }
) ->
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    RcvMax = maps:get(receive_maximum, ConnInfo, 0),
    ExpiryInterval = maps:get(expiry_interval, ConnInfo, 0),
    with_basic_columns(
        'client.connect',
        #{
            peername => ntoa(PeerName),
            sockname => ntoa(SockName),
            proto_name => ProtoName,
            proto_ver => ProtoVer,
            keepalive => Keepalive,
            clean_start => CleanStart,
            receive_maximum => RcvMax,
            expiry_interval => ExpiryInterval div 1000,
            conn_props => printable_maps(ConnProps)
        }
    ).

eventmsg_connected(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        is_bridge := IsBridge,
        mountpoint := Mountpoint
    },
    ConnInfo = #{
        peername := PeerName,
        sockname := SockName,
        clean_start := CleanStart,
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        connected_at := ConnectedAt
    }
) ->
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    RcvMax = maps:get(receive_maximum, ConnInfo, 0),
    ExpiryInterval = maps:get(expiry_interval, ConnInfo, 0),
    with_basic_columns(
        'client.connected',
        #{
            clientid => ClientId,
            username => Username,
            mountpoint => Mountpoint,
            peername => ntoa(PeerName),
            sockname => ntoa(SockName),
            proto_name => ProtoName,
            proto_ver => ProtoVer,
            keepalive => Keepalive,
            clean_start => CleanStart,
            receive_maximum => RcvMax,
            expiry_interval => ExpiryInterval div 1000,
            is_bridge => IsBridge,
            conn_props => printable_maps(ConnProps),
            connected_at => ConnectedAt
        }
    ).

eventmsg_disconnected(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username
    },
    ConnInfo = #{
        peername := PeerName,
        sockname := SockName,
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        disconnected_at := DisconnectedAt
    },
    Reason
) ->
    with_basic_columns(
        'client.disconnected',
        #{
            reason => reason(Reason),
            clientid => ClientId,
            username => Username,
            peername => ntoa(PeerName),
            sockname => ntoa(SockName),
            proto_name => ProtoName,
            proto_ver => ProtoVer,
            disconn_props => printable_maps(maps:get(disconn_props, ConnInfo, #{})),
            disconnected_at => DisconnectedAt
        }
    ).

eventmsg_connack(
    ConnInfo = #{
        clientid := ClientId,
        clean_start := CleanStart,
        username := Username,
        peername := PeerName,
        sockname := SockName,
        proto_name := ProtoName,
        proto_ver := ProtoVer
    },
    Reason
) ->
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    ExpiryInterval = maps:get(expiry_interval, ConnInfo, 0),
    with_basic_columns(
        'client.connack',
        #{
            reason_code => reason(Reason),
            clientid => ClientId,
            clean_start => CleanStart,
            username => Username,
            peername => ntoa(PeerName),
            sockname => ntoa(SockName),
            proto_name => ProtoName,
            proto_ver => ProtoVer,
            keepalive => Keepalive,
            expiry_interval => ExpiryInterval,
            conn_props => printable_maps(ConnProps)
        }
    ).

eventmsg_authenticate(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost
    },
    Result
) ->
    with_basic_columns(
        'client.authenticate',
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost),
            result => Result
        }
    ).

eventmsg_authorize(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost
    },
    PubSub,
    Topic,
    Result
) ->
    with_basic_columns(
        'client.authorize',
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost),
            topic => Topic,
            action => PubSub,
            result => Result
        }
    ).

eventmsg_check_authz_complete(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost
    },
    PubSub,
    Topic,
    Result,
    AuthzSource
) ->
    with_basic_columns(
        'client.check_authz_complete',
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost),
            topic => Topic,
            action => PubSub,
            authz_source => AuthzSource,
            result => Result
        }
    ).

eventmsg_session_created(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost
    },
    SessionId,
    CreatedAt
) ->
    with_basic_columns(
        'session.created',
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost),
            session_id => SessionId,
            created_at => CreatedAt
        }
    ).

eventmsg_sub_or_unsub(
    Event,
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost
    },
    Topic,
    SubOpts = #{qos := QoS}
) ->
    PropKey = sub_unsub_prop_key(Event),
    with_basic_columns(
        Event,
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost),
            PropKey => printable_maps(maps:get(PropKey, SubOpts, #{})),
            topic => Topic,
            qos => QoS
        }
    ).

eventmsg_session(
    Evt,
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost
    }
) ->
    with_basic_columns(
        Evt,
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost)
        }
    ).

eventmsg_session_terminated(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost
    },
    Reason
) ->
    with_basic_columns(
        'session.terminated',
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost),
            reason => reason(Reason)
        }
    ).

eventmsg_dropped(
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp
    },
    ByNode,
    Reason
) ->
    with_basic_columns(
        'message.dropped',
        #{
            id => emqx_guid:to_hexstr(Id),
            by_node => ByNode,
            reason => Reason,
            clientid => ClientId,
            username => emqx_message:get_header(username, Message, undefined),
            payload => Payload,
            peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined)),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        }
    ).

eventmsg_delivered(
    _ClientInfo = #{
        peerhost := PeerHost,
        clientid := ReceiverCId,
        username := ReceiverUsername
    },
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp
    }
) ->
    with_basic_columns(
        'message.delivered',
        #{
            id => emqx_guid:to_hexstr(Id),
            from_clientid => ClientId,
            from_username => emqx_message:get_header(username, Message, undefined),
            clientid => ReceiverCId,
            username => ReceiverUsername,
            payload => Payload,
            peerhost => ntoa(PeerHost),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        }
    ).

eventmsg_acked(
    _ClientInfo = #{
        peerhost := PeerHost,
        clientid := ReceiverCId,
        username := ReceiverUsername
    },
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp
    }
) ->
    with_basic_columns(
        'message.acked',
        #{
            id => emqx_guid:to_hexstr(Id),
            from_clientid => ClientId,
            from_username => emqx_message:get_header(username, Message, undefined),
            clientid => ReceiverCId,
            username => ReceiverUsername,
            payload => Payload,
            peerhost => ntoa(PeerHost),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            puback_props => printable_maps(emqx_message:get_header(puback_props, Message, #{})),
            publish_received_at => Timestamp
        }
    ).

with_basic_columns(EventName, Columns) when is_map(Columns) ->
    Columns#{
        event => EventName,
        timestamp => erlang:system_time(millisecond),
        node => node()
    }.

reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

ntoa(undefined) -> undefined;
ntoa({IpAddr, Port}) -> iolist_to_binary([inet:ntoa(IpAddr), ":", integer_to_list(Port)]);
ntoa(IpAddr) -> iolist_to_binary(inet:ntoa(IpAddr)).

sub_unsub_prop_key('session.subscribed') -> sub_props;
sub_unsub_prop_key('session.unsubscribed') -> unsub_props.

printable_maps(undefined) ->
    #{};
printable_maps(Headers) ->
    maps:fold(
        fun
            (K, V0, AccIn) when K =:= peerhost; K =:= peername; K =:= sockname ->
                AccIn#{K => ntoa(V0)};
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{
                    %% The 'User-Property' field is for the convenience of querying properties
                    %% using the '.' syntax, e.g. "SELECT 'User-Property'.foo as foo"
                    %% However, this does not allow duplicate property keys. To allow
                    %% duplicate keys, we have to use the 'User-Property-Pairs' field instead.
                    'User-Property' => maps:from_list(V0),
                    'User-Property-Pairs' => [
                        #{
                            key => Key,
                            value => Value
                        }
                        || {Key, Value} <- V0
                    ]
                };
            (_K, V, AccIn) when is_tuple(V) ->
                %% internal headers
                AccIn;
            (K, V, AccIn) ->
                AccIn#{K => V}
        end,
        #{'User-Property' => #{}},
        Headers
    ).