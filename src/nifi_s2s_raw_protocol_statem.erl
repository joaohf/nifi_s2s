%%%-------------------------------------------------------------------
%% @doc Site 2 Site client state machine.
%% @end
%%%-------------------------------------------------------------------
-module(nifi_s2s_raw_protocol_statem).

-behaviour(gen_statem).

-export([start_link/2, stop/1, get_peer_list/1]).
-export([callback_mode/0, init/1]).
-export([idle/3, established/3, handshaked/3, get_peer_list/3]).

-include("nifi_s2s.hrl").

-record(raw_s2s_protocol, {
    mode :: peer_list | peer,
    % Batch Count
    batch_count :: non_neg_integer(),
    % Batch Size
    batch_size :: non_neg_integer(),
    % Batch Duration in msec
    batch_duration :: non_neg_integer(),
    % Timeout in msec
    timeout :: non_neg_integer(),
    % commsIdentifier
    comms_identifier :: string(),

    client :: client(),

    from :: any()
}).

-define(RESOURCE_NAME, <<"SocketFlowFileProtocol">>).
-define(CODEC_RESOURCE_NAME, <<"StandardFlowFileCodec">>).

% handshake properties

% Boolean value indicating whether or not the contents of a FlowFile should
% be GZipped when transferred.
-define(GZIP, <<"GZIP">>).

% The unique identifier of the port to communicate with
-define(PORT_IDENTIFIER, <<"PORT_IDENTIFIER">>).

% Indicates the number of milliseconds after the request was made that the
% client will wait for a response. If no response has been received by the
% time this value expires, the server can move on without attempting to
% service the request because the client will have already disconnected.
-define(REQUEST_EXPIRATION_MILLIS, <<"REQUEST_EXPIRATION_MILLIS">>).

% The preferred number of FlowFiles that the server should send to the
% client when pulling data. This property was introduced in version 5 of
% the protocol.
-define(BATCH_COUNT, <<"BATCH_COUNT">>).


% The preferred number of bytes that the server should send to the client
% when pulling data. This property was introduced in version 5 of the
% protocol.
-define(BATCH_SIZE, <<"BATCH_SIZE">>).

% The preferred amount of time that the server should send data to the
% client when pulling data. This property was introduced in version 5 of
% the protocol. Value is in milliseconds.
-define(BATCH_DURATION, <<"BATCH_DURATION">>).


start_link(Client, Mode) ->
    gen_statem:start_link(?MODULE, {Client, Mode}, []).


stop(Client) ->
    gen_statem:stop(Client).

%%
%% @doc Obtains the peer list.
%% @end
%%
get_peer_list(Pid) -> 
    gen_statem:call(Pid, {get_peer_list, []}).

%% gen_statem callbacks

callback_mode() -> [state_functions, state_enter].

init({Client, Mode}) ->
    Data = #raw_s2s_protocol{
        mode = Mode,
        batch_count = 0,
        batch_size = 0,
        batch_duration = 0,
        timeout = 30000,
        client = Client
    },
    Actions = [{next_event, internal, establish}],
    {ok, idle, Data, Actions}.


idle(enter, idle, _Data) ->
    keep_state_and_data;

idle(enter, get_peer_list, _Data) ->
    {stop, normal};

idle(enter, handshaked, #raw_s2s_protocol{client = Client} = Data) ->
    %% writeRequestType(SHUTDOWN);

    %%  known_transactions_.clear();
    
    Peer = nifi_s2s_peer:close(Client#client.peer),
    NData = Data#raw_s2s_protocol{client = Client#client{peer = Peer}},

    Actions = [{next_event, internal, establish}],
    {keep_state, idle, NData, Actions};
  
idle(enter, _OldState, #raw_s2s_protocol{client = Client} = Data) ->
    Peer = nifi_s2s_peer:close(Client#client.peer),
    NData = Data#raw_s2s_protocol{client = Client#client{peer = Peer}},

    Actions = [{next_event, internal, establish}],
    {keep_state, idle, NData, Actions};

idle(internal, establish, #raw_s2s_protocol{client = Client} = Data) ->
    case nifi_s2s_peer:open(Client#client.peer) of
        {ok, Peer} ->
            NData = Data#raw_s2s_protocol{client = Client#client{peer = Peer}},
            Actions = [{next_event, internal, initiate_resource_negotiation}],
            {next_state, established, NData, Actions};
        {error, _Error} ->
            keep_state_and_data
    end.

%% Established state

established(enter, idle, _Data) ->
    keep_state_and_data;

established(internal, initiate_resource_negotiation, Data) ->
    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, ?RESOURCE_NAME),

    Value = <<(Data#raw_s2s_protocol.client#client.currentVersion):32/integer-big>>,

    ok = nifi_s2s_peer:write(Data#raw_s2s_protocol.client#client.peer, Value),

    ok = inet:setopts(Data#raw_s2s_protocol.client#client.peer#s2s_peer.stream, [{active, once}]),

    {keep_state, Data};

established(info, {tcp, Socket, Packet}, #raw_s2s_protocol{client = #client{peer = #s2s_peer{stream = Socket}}} = Data) ->

    StatusCode = decode_status_code(Packet),

    case StatusCode of
        'RESOURCE_OK' ->
            Actions = [{next_event, internal, handshake}],
            {next_state, handshaked, Data, Actions};
        'DIFFERENT_RESOURCE_VERSION' ->
            Actions = [{next_event, internal, initiate_resource_negotiation}],
            {keep_state, Data, Actions};
        'NEGOTIATED_ABORT' ->
            {stop, StatusCode}
    end.


handshaked(enter, established, _Data) -> keep_state_and_data;

handshaked(internal, handshake, Data) ->
    CommsIdentifier = nifi_s2s_utils:uuid(),

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, CommsIdentifier),

    PropertiesVersion = 
    case Data#raw_s2s_protocol.client#client.currentVersion of
        Version when Version >= 5 ->
          M0 = version5(Data, bs, #{}),
          M1 = version5(Data, bc, M0), 
          version5(Data, bd, M1);
        _Version ->
          #{}
    end,

    PropertiesBase = #{
        ?GZIP => <<"false">>,
        %?PORT_IDENTIFIER => Data#raw_s2s_protocol.client#client.port_id_str,
        ?REQUEST_EXPIRATION_MILLIS => integer_to_binary(Data#raw_s2s_protocol.timeout)
     },

     Properties = maps:merge(PropertiesBase, PropertiesVersion),

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer,
        Data#raw_s2s_protocol.client#client.peer#s2s_peer.url),

    ok = write_properties(Data#raw_s2s_protocol.client#client.peer, Properties),

    ok = inet:setopts(Data#raw_s2s_protocol.client#client.peer#s2s_peer.stream, [{active, once}]),

    {keep_state, Data#raw_s2s_protocol{comms_identifier = CommsIdentifier}};

handshaked(info, {tcp, Socket, Packet}, #raw_s2s_protocol{client = #client{peer = #s2s_peer{stream = Socket}}} = Data) ->
    #raw_s2s_protocol{mode = Mode} = Data,
    RespondCode = decode_response_code(Packet),

    case RespondCode of
        'PROPERTIES_OK' ->
            case Mode of
                peer_list ->
                    %keep_state_and_data;
                    %Actions = [{next_event, internal, negotiate_codec}],
                    {next_state, get_peer_list, Data};
                peer ->
                    Actions = [{next_event, internal, negotiate_codec}],
                    {next_state, ready, Data, Actions}
            end;
        'PORT_NOT_IN_VALID_STATE' ->
            {stop, RespondCode};
        'UNKNOWN_PORT' ->
            {stop, RespondCode};
        'PORTS_DESTINATION_FULL' ->
            {stop, RespondCode};
        _Error ->
            {stop, RespondCode}
    end;

handshaked({call, From}, {get_peer_list, []}, #raw_s2s_protocol{client = #client{peer = #s2s_peer{stream = Socket}}} = Data) ->

    {keep_state, Data};

handshaked(info, {tcp, Socket, Packet}, #raw_s2s_protocol{client = #client{peer = #s2s_peer{stream = Socket}}} = Data) ->

    %gen_statem:reply(From, {ok, Response}),

    {keep_state, Data}.


get_peer_list(enter, handshaked, _Data) ->
    keep_state_and_data;

get_peer_list({call, From}, {get_peer_list, []}, #raw_s2s_protocol{} = Data) ->

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, ?REQUEST_PEER_LIST),

    ok = inet:setopts(Data#raw_s2s_protocol.client#client.peer#s2s_peer.stream, [{active, once}]),

    {keep_state, Data#raw_s2s_protocol{from = From}};

get_peer_list(info, {tcp, Socket, Packet}, #raw_s2s_protocol{from = From, client = #client{peer = #s2s_peer{stream = Socket}}} = Data) ->
    
    ct:pal("xxxxx ~p", [Packet]),

    Peers = decode_peer_list(Packet),

    ok = gen_statem:reply(From, {ok, Peers}),

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, ?SHUTDOWN),

    nifi_s2s_peer:close(Data#raw_s2s_protocol.client#client.peer),

    {next_state, idle, Data}.


ready(internal, negotiate_code, Data) ->
    ok.


version5(#raw_s2s_protocol{batch_count = Bc}, bc, Properties) when Bc > 0 ->
    maps:put(?BATCH_COUNT, integer_to_binary(Bc), Properties);

version5(#raw_s2s_protocol{}, bc, Properties) ->
    Properties;

version5(#raw_s2s_protocol{batch_size = Bs}, bs, Properties) when Bs > 0 ->
    maps:put(?BATCH_SIZE, integer_to_binary(Bs), Properties);

version5(#raw_s2s_protocol{}, bs, Properties) ->
    Properties;

version5(#raw_s2s_protocol{batch_duration = Bd}, bd, Properties) when Bd > 0 ->
    maps:put(?BATCH_DURATION, integer_to_binary(Bd), Properties);

version5(#raw_s2s_protocol{}, bd, Properties) ->
    Properties.


write_url(Peer, CurrentVersion) when CurrentVersion >=3 ->
    Url = nifi_s2s_peer:get_url(Peer),
    nifi_s2s_peer:write(Peer, Url);

write_url(_Peer, _CurrentVersion) ->
    ok.


write_properties(Peer, Map) ->
    Fun = fun(Key, Value, AccIn) ->
        KLen = byte_size(Key),
        VLen = byte_size(Value),
        K = <<KLen:16/integer-big, Key/binary>>,
        V = <<VLen:16/integer-big, Value/binary>>,
        [ K,V | AccIn ]
    end,
    Properties = maps:fold(Fun, [], Map),
    Size = <<(maps:size(Map)):32/integer-big>>,
    nifi_s2s_peer:write(Peer, [Size | Properties]).


decode_status_code(<<?RESOURCE_OK>>) ->
    'RESOURCE_OK';

decode_status_code(<<?DIFFERENT_RESOURCE_VERSION>>) ->
    'DIFFERENT_RESOURCE_VERSION';

decode_status_code(<<?NEGOTIATED_ABORT>>) ->
    'NEGOTIATED_ABORT'.


decode_response_code(<<$R, $C, Code:8/integer>>) ->
    decode_response_code(Code);

decode_response_code(?PROPERTIES_OK) ->
    'PROPERTIES_OK';

decode_response_code(?UNKNOWN_PROPERTY_NAME) ->
    'UNKNOWN_PROPERTY_NAME';

decode_response_code(?ILLEGAL_PROPERTY_VALUE) ->
    'ILLEGAL_PROPERTY_VALUE';

decode_response_code(?MISSING_PROPERTY) ->
    'MISSING_PROPERTY'.


decode_peer_list(<<Size:32, _/binary>>) when Size == 0 ->
    [];

decode_peer_list(<<Size:32/integer-big, Rest/binary>>) ->
    NPeers = lists:seq(0, Size-1),
    lists:foldr(fun decode_peer/2, {Rest, []}, NPeers).


decode_peer(0, {<<HostSize:16/integer, Host:HostSize/binary, Port:32/integer, 
              Secure:8/integer, Count:32/integer>>, Peers}) ->
    Peer = peer({Host, Port, Secure, Count}),
    [Peer | Peers];

decode_peer(_Elem, {<<HostSize:16/integer, Host:HostSize/binary, Port:32/integer, 
              Secure:8/integer, Count:32/integer, Rest/binary>>, Peers}) ->

    Peer = peer({Host, Port, Secure, Count}),
    {Rest, [Peer | Peers]}.


peer({Host, Port, Secure, Count}) ->
    _Secure =
    case Secure of
        0 -> false;
        1 -> true
    end,

    Status = #peer_status{flow_file_count = Count,
                          query_for_peers = true},
    #peer{host = binary_to_list(Host),
          port = Port,
          secure = _Secure, 
          status = Status
    }.