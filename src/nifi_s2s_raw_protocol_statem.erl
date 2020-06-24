%%%-------------------------------------------------------------------
%% @doc Site 2 Site client state machine.
%% @end
%% @private
%%%-------------------------------------------------------------------
-module(nifi_s2s_raw_protocol_statem).

-behaviour(gen_statem).

-export([start_link/2, start_link/3,
         stop/1,
         get_peer_list/1,
         transmit_payload/3,
         transfer_flowfile/2,
         receive_flowfiles/1]).

-export([callback_mode/0,
         init/1,
         terminate/3]).

-export([idle/3,
         established/3,
         handshaked/3,
         get_peer_list/3,
         ready/3]).

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

    from :: any(),

    transaction = undefined :: pid(),
    transaction_mon_ref :: reference()
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
    gen_statem:start_link(?MODULE, {Client, Mode, []}, []).

start_link(Client, Mode, Options) ->
    gen_statem:start_link(?MODULE, {Client, Mode, Options}, []).


stop(Client) ->
    gen_statem:stop(Client).

%%
%% @doc Obtains the peer list.
%% @end
%%
get_peer_list(Pid) ->
    gen_statem:call(Pid, {get_peer_list, []}).


transmit_payload(Pid, Payload, Attributes) ->
    gen_statem:call(Pid, {transmit_payload, Payload, Attributes}).


transfer_flowfile(Pid, Flowfiles) ->
    gen_statem:call(Pid, {transfer_flowfile, Flowfiles}).


receive_flowfiles(Pid) ->
    gen_statem:call(Pid, {receive_flowfiles, []}).

%% gen_statem callbacks

callback_mode() -> [state_functions, state_enter].

init({Client, Mode, Opts}) ->
    Data = #raw_s2s_protocol{
        mode = Mode,
        batch_count = proplists:get_value(batch_count, Opts, 0),
        batch_size = proplists:get_value(batch_size, Opts, 0),
        batch_duration = proplists:get_value(batch_duration, Opts, 0),
        timeout = proplists:get_value(timeout, Opts, 30000),
        client = Client
    },
    Actions = [{next_event, internal, establish}],
    {ok, idle, Data, Actions}.


terminate(normal, ready, #raw_s2s_protocol{client = #client{peer = Peer}}) ->
    ok = write_request_type(Peer, ?SHUTDOWN),    
    _ = nifi_s2s_peer:close(Peer),

    ok;

terminate(_, _, _Data) ->
    ok.


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

idle({call, _From}, {get_peer_list, []}, #raw_s2s_protocol{}) ->
    Actions = [postpone],
    {keep_state_and_data, Actions};

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

established({call, _From}, _, #raw_s2s_protocol{}) ->
    Actions = [postpone],
    {keep_state_and_data, Actions};

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
    CommsIdentifier = uuid:get_v4(),

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, CommsIdentifier),

    Version = Data#raw_s2s_protocol.client#client.currentVersion,

    M0 = protocol_version(Version, Data, ?BATCH_COUNT, #{}),
    M1 = protocol_version(Version, Data, ?BATCH_DURATION, M0),
    M2 = protocol_version(Version, Data, ?BATCH_SIZE, M1),

    PropertiesBase = #{
        ?GZIP => <<"false">>,
        ?PORT_IDENTIFIER => list_to_binary(Data#raw_s2s_protocol.client#client.port_id),
        ?REQUEST_EXPIRATION_MILLIS => integer_to_binary(Data#raw_s2s_protocol.timeout)
     },

    Properties = maps:merge(PropertiesBase, M2),

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

handshaked({call, _From}, _, #raw_s2s_protocol{}) ->
    Actions = [postpone],
    {keep_state_and_data, Actions}.


get_peer_list(enter, handshaked, _Data) ->
    keep_state_and_data;

get_peer_list({call, From}, {get_peer_list, []}, #raw_s2s_protocol{} = Data) ->
    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, ?REQUEST_PEER_LIST),

    ok = inet:setopts(Data#raw_s2s_protocol.client#client.peer#s2s_peer.stream, [{active, once}]),

    {keep_state, Data#raw_s2s_protocol{from = From}};

get_peer_list(info, {tcp, Socket, Packet}, #raw_s2s_protocol{from = From, client = #client{peer = #s2s_peer{stream = Socket}}} = Data) ->
    Peers = decode_peer_list(Packet),

    ok = gen_statem:reply(From, {ok, Peers}),

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, ?SHUTDOWN),

    nifi_s2s_peer:close(Data#raw_s2s_protocol.client#client.peer),

    {next_state, idle, Data}.


ready(enter, handshaked, _Data) ->
    keep_state_and_data;

ready(internal, negotiate_codec, Data) ->

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, ?NEGOTIATE_FLOWFILE_CODEC),

    ok = nifi_s2s_peer:write_utf(Data#raw_s2s_protocol.client#client.peer, ?CODEC_RESOURCE_NAME),

    Value = <<(Data#raw_s2s_protocol.client#client.currentCodecVersion):32/integer-big>>,
    ok = nifi_s2s_peer:write(Data#raw_s2s_protocol.client#client.peer, Value),

    ok = inet:setopts(Data#raw_s2s_protocol.client#client.peer#s2s_peer.stream, [{active, once}]),

    keep_state_and_data;

ready(info, {tcp, Socket, Packet}, #raw_s2s_protocol{
    client = #client{peer = #s2s_peer{stream = Socket}}} = Data) ->

    StatusCode = decode_status_code(Packet),

    case StatusCode of
        'RESOURCE_OK' ->
            {keep_state, Data};
        'DIFFERENT_RESOURCE_VERSION' ->
            Actions = [{next_event, internal, negotiate_codec}],
            {keep_state, Data, Actions};
        'NEGOTIATED_ABORT' ->
            {stop, StatusCode}
    end;

ready({call, From}, {transmit_payload, Payload, Attributes}, #raw_s2s_protocol{} = Data) ->
    Peer = Data#raw_s2s_protocol.client#client.peer,

    {ok, {Transaction, Ref}} = create_transaction(Peer, ?TRANSFER_DIRECTION_SEND),

    Packet = #data_packet{
        payload = Payload,
        attributes = Attributes
    },

    ok = nifi_s2s_transaction_statem:send(Transaction, Packet, undefined),

    % confirm includes complete
    ok = nifi_s2s_transaction_statem:confirm(Transaction),

    NData = Data#raw_s2s_protocol{from = From,
     transaction = {?TRANSFER_DIRECTION_SEND, Transaction},
     transaction_mon_ref = Ref},

    {keep_state, NData};

ready({call, From}, {transfer_flowfile, Flowfiles}, #raw_s2s_protocol{} = Data) ->
    Peer = Data#raw_s2s_protocol.client#client.peer,

    {ok, {Transaction, Ref}} = create_transaction(Peer, ?TRANSFER_DIRECTION_SEND),

    ok = do_send(Transaction, Flowfiles),
    
    NData = Data#raw_s2s_protocol{from = undefined,
     transaction = {?TRANSFER_DIRECTION_SEND, Transaction},
     transaction_mon_ref = Ref},

    Actions = [{reply, From, {ok, Transaction}}],
    {keep_state, NData, Actions};

ready({call, From}, {receive_flowfiles, []}, #raw_s2s_protocol{} = Data) ->
    Peer = Data#raw_s2s_protocol.client#client.peer,

    {ok, {Transaction, Ref}} = create_transaction(Peer, ?TRANSFER_DIRECTION_RECEIVE),

    ok =
    case nifi_s2s_client:read_response(Peer) of
        'MORE_DATA' ->
            nifi_s2s_transaction_statem:set_data_available(Transaction, true);
        'NO_MORE_DATA' ->
            nifi_s2s_transaction_statem:set_data_available(Transaction, false)
    end,
    
    Flowfiles = do_receiver(Transaction),

    % confirm includes complete
    ok = nifi_s2s_transaction_statem:confirm(Transaction),

    NData = Data#raw_s2s_protocol{from = undefined,
     transaction = {?TRANSFER_DIRECTION_RECEIVE, Transaction},
     transaction_mon_ref = Ref},


    Actions = [{reply, From, {ok, Flowfiles}}],
    {keep_state, NData, Actions};

ready(info, {'DOWN', MonitorRef, process, Transaction, normal},
    #raw_s2s_protocol{transaction_mon_ref = MonitorRef, transaction = { _, Transaction}} = Data) ->

    NData = Data#raw_s2s_protocol{transaction_mon_ref = undefined,
     transaction = undefined,
     from = undefined},

    Actions =
    case Data#raw_s2s_protocol.from of
        undefined -> [];
        _ -> [{reply, Data#raw_s2s_protocol.from, ok}]
    end,
    {keep_state, NData, Actions}.

%
% Helper functions
%

%% Send flowfiles
do_send(_Transaction, {empty, _Flowfiles}) ->
    ok;

do_send(Transaction, {{value, Flowfile}, Flowfiles}) ->
    Packet = #data_packet{
        payload = undefined,
        attributes = nifi_flowfile:get_attributes(Flowfile)
    },

    ok = nifi_s2s_transaction_statem:send(Transaction, Packet, Flowfile),

    do_send(Transaction, nifi_flowfile:remove(Flowfiles));

do_send(Transaction, Flowfiles) ->
    do_send(Transaction, nifi_flowfile:remove(Flowfiles)).


%% Receive data packets and build flowfiles
do_receiver(Transaction) ->
    do_receiver(Transaction, nifi_s2s_transaction_statem:receiver(Transaction),
        nifi_flowfile:new()).

do_receiver(_Transaction, {ok, eof}, AccIn) ->
    AccIn;
    
do_receiver(Transaction, {ok, #data_packet{attributes = Att, payload = Payload}}, AccIn) ->
    Flowfile = nifi_flowfile:new(Att, Payload),
    do_receiver(Transaction, nifi_s2s_transaction_statem:receiver(Transaction),
        nifi_flowfile:add(Flowfile, AccIn)).


write_request_type(Peer, Type) ->
    nifi_s2s_peer:write_utf(Peer, Type).


create_transaction(Peer, ?TRANSFER_DIRECTION_RECEIVE) ->
    {ok, {_Pid, _Ref}} = Result = nifi_s2s_transaction_statem:create(Peer, ?TRANSFER_DIRECTION_RECEIVE),

    ok = nifi_s2s_peer:write_utf(Peer, ?RECEIVE_FLOWFILES),

    Result;

create_transaction(Peer, ?TRANSFER_DIRECTION_SEND) ->    
    {ok, {_Pid, _Ref}} = Result = nifi_s2s_transaction_statem:create(Peer, ?TRANSFER_DIRECTION_SEND),

    ok = nifi_s2s_peer:write_utf(Peer, ?SEND_FLOWFILES),

    Result.


protocol_version(5, #raw_s2s_protocol{batch_count = Value}, ?BATCH_COUNT = Key, Map) when Value > 0 ->
    maps:put(Key, integer_to_binary(Value), Map);

protocol_version(5, #raw_s2s_protocol{batch_size = Value}, ?BATCH_SIZE = Key, Map) when Value > 0 ->
    maps:put(Key, integer_to_binary(Value), Map);

protocol_version(5, #raw_s2s_protocol{batch_duration = Value}, ?BATCH_DURATION = Key, Map) when Value > 0 ->
    maps:put(Key, integer_to_binary(Value), Map);

protocol_version(_, #raw_s2s_protocol{}, _, Map) ->
    Map.


write_url(Peer, CurrentVersion) when CurrentVersion >= 3 ->
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
    'NEGOTIATED_ABORT';

decode_status_code(<<?NEGOTIATED_ABORT:8/integer, _Rest/bitstring>>) ->
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
    'MISSING_PROPERTY';

decode_response_code(?UNKNOWN_PORT) ->
    'UNKNOWN_PORT'.


decode_peer_list(<<Size:32, _/binary>>) when Size == 0 ->
    [];

decode_peer_list(<<Size:32/integer-big, Rest/binary>>) ->
    NPeers = lists:seq(0, Size-1),
    lists:foldr(fun decode_peer/2, {Rest, []}, NPeers).


decode_peer(0, {<<HostSize:16/integer, Host:HostSize/binary, Port:32/integer, 
              Secure:8/integer, Count:32/integer>>, Peers}) ->
    [peer({Host, Port, Secure, Count}) | Peers];

decode_peer(_Elem, {<<HostSize:16/integer, Host:HostSize/binary, Port:32/integer, 
              Secure:8/integer, Count:32/integer, Rest/binary>>, Peers}) ->
    {Rest, [peer({Host, Port, Secure, Count}) | Peers]}.


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