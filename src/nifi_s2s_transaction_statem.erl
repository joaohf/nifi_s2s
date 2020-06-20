-module(nifi_s2s_transaction_statem).

-behaviour(gen_statem).

-export([create/2,
         send/3,
         receiver/1,
         confirm/1,
         delete/1,
         set_data_available/2]).

-export([callback_mode/0,
         init/1,
         terminate/3]).

-export([transaction_started/3,
         data_exchanged/3,
         transaction_confirmed/3,
         transaction_completed/3,
         transaction_canceled/3,
         transaction_closed/3,
         transaction_error/3]).

-include("nifi_s2s.hrl").

-record(transaction, {
      % Number of current transfers
      current_transfers  = 0 :: non_neg_integer(),
      % number of total seen transfers
      total_transfers = 0 :: non_neg_integer(),
      % Number of content bytes
      bytes = 0 :: non_neg_integer(),
      
      % Whether received data is available
      data_available = undefined :: boolean() | undefined,

      %  // Transaction Direction TransferDirection
      direction :: 'send' | 'receive',

      % A global unique identifier
      uuid :: any(),

      peer :: s2s_peer(),

      crc :: non_neg_integer(),

      from = undefined:: {To :: pid(), Tag :: term()},

      current_version :: non_neg_integer()
}).


create(Peer, Direction) ->
    {ok, {_Pid, _Ref}} = gen_statem:start_monitor(?MODULE, {Peer, Direction}, []).


send(Transaction, Packet, Flowfile) ->
    gen_statem:call(Transaction, {send, Packet, Flowfile}).


receiver(Transaction) ->
    gen_statem:call(Transaction, {receiver, []}).


confirm(Transaction) ->
    gen_statem:call(Transaction, {confirm, []}).


delete(Transaction) ->
    gen_statem:stop(Transaction).


set_data_available(Transaction, Value) ->
    gen_statem:call(Transaction, {set_data_available, Value}).

%% gen_statem callbacks

callback_mode() -> [state_functions].

init({Peer, Direction}) ->
    TransactionId = uuid:get_v4(),
    Data = #transaction{peer = Peer, uuid = TransactionId, crc = 0, direction = Direction},
    Actions = [{next_event, internal, new}],
    {ok, transaction_started, Data, Actions}.


terminate(_, _, _Data) ->
    ok.


transaction_started(internal, new, #transaction{}) ->
    keep_state_and_data;

transaction_started({call, From}, {set_data_available, Value}, Data) ->
    Actions = [{reply, From, ok}],
    {next_state, data_exchanged, Data#transaction{data_available = Value}, Actions};

transaction_started({call, From}, {receiver, []}, #transaction{data_available = false} = Data) ->
   Actions = [{reply, From, {ok, eof}}],
   {next_state, data_exchanged, Data#transaction{from = From}, Actions};

transaction_started({call, From}, {receiver, []}, #transaction{data_available = true} = Data) ->

   {NData, PacketOrEOF} = do_receive(Data),   
   
   Actions = [{reply, From, {ok, PacketOrEOF}}],
   {next_state, data_exchanged, NData, Actions};

transaction_started({call, From}, {send, Packet, Flowfile},
    #transaction{} = Data) ->

    NData = do_send(Data, Packet, Flowfile),

    Actions = [{reply, From, ok}],
    {next_state, data_exchanged, NData, Actions}.

data_exchanged({call, From}, {receiver, []}, #transaction{data_available = false, current_transfers = _N} = Data) ->

    {NData, PacketOrEOF} = do_receive(Data),

    Actions = [{reply, From, {ok, eof}}],
    {keep_state, NData, Actions};

data_exchanged({call, From}, {receiver, []}, #transaction{data_available = true} = Data) ->

    {NData, PacketOrEOF} = do_receive(Data),

    DataAvailable =
    case nifi_s2s_client:read_response(Data#transaction.peer) of
        'CONTINUE_TRANSACTION' ->
            true;
        'FINISH_TRANSACTION' ->
            false
    end,

    Actions = [{reply, From, {ok, PacketOrEOF}}],
    {keep_state, NData#transaction{data_available = DataAvailable}, Actions};


data_exchanged({call, From}, {send, Packet, Flowfile},
    #transaction{direction = ?TRANSFER_DIRECTION_SEND, peer = Peer,
     current_transfers = Ct} = Data) when Ct > 0 ->
    
    ok = nifi_s2s_client:write_response(Peer,
        ?CONTINUE_TRANSACTION, <<"CONTINUE_TRANSACTION">>),
    
    NData = do_send(Data, Packet, Flowfile),

    Actions = [{reply, From, ok}],
    {keep_state, NData, Actions};

data_exchanged({call, From}, {confirm, []},
    #transaction{direction = ?TRANSFER_DIRECTION_RECEIVE, data_available = true} = Data) ->
    Actions = [{next_event, internal, complete}],
    {next_state, transaction_confirmed, Data#transaction{from = From}, Actions};

data_exchanged({call, From}, {confirm, []},
    #transaction{peer = Peer, direction = ?TRANSFER_DIRECTION_RECEIVE, data_available = false, current_transfers = 0} = Data) ->
    Actions = [{next_event, internal, complete}],
    {next_state, transaction_confirmed, Data#transaction{from = From}, Actions};    

data_exchanged({call, From}, {confirm, []},
    #transaction{peer = Peer, direction = ?TRANSFER_DIRECTION_RECEIVE, data_available = false, crc = Crc} = Data) ->

    CrcBin = integer_to_binary(Crc),

    ok = nifi_s2s_client:write_response(Peer, ?CONFIRM_TRANSACTION, CrcBin),

    case nifi_s2s_client:read_response(Peer) of
        {'CONFIRM_TRANSACTION', _} ->
            Actions = [{next_event, internal, complete}],
            {next_state, transaction_confirmed, Data#transaction{from = From}, Actions};
        'BAD_CHECKSUM' ->
            {stop, bad_checksum};
        Code ->
            {stop, {unknow_code, Code}}
    end;

data_exchanged({call, From}, {confirm, []},
    #transaction{direction = ?TRANSFER_DIRECTION_SEND, peer = Peer, crc = Crc,
    current_version = Cv} = Data) ->

    CrcBin = integer_to_binary(Crc),

    ok = nifi_s2s_client:write_response(Peer, ?FINISH_TRANSACTION),

    % take into account the protocol version
    case nifi_s2s_client:read_response(Peer) of
        {'CONFIRM_TRANSACTION', CrcT} when CrcT =:= CrcBin, Cv >= 3 ->
            ok = nifi_s2s_client:write_response(Peer,
                ?CONFIRM_TRANSACTION, <<>>),

            Actions = [{next_event, internal, complete}],
            {next_state, transaction_confirmed, Data#transaction{from = From}, Actions};

        {'CONFIRM_TRANSACTION', _C} when Cv >= 3 ->
            ok = nifi_s2s_client:write_response(Peer,
                ?BAD_CHECKSUM, <<"BAD_CHECKSUM">>),

            {stop, bad_checksum};

        'CONFIRM_TRANSACTION' ->
            ok = nifi_s2s_client:write_response(Peer,
                ?CONFIRM_TRANSACTION, <<>>),

            Actions = [{next_event, internal, complete}],
            {next_state, transaction_confirmed, Data#transaction{from = From}, Actions}
    end.


transaction_confirmed(internal, complete, 
    #transaction{direction = ?TRANSFER_DIRECTION_RECEIVE, current_transfers = 0} = Data) ->
    Actions = [{next_event, internal, delete}],
    {next_state, transaction_completed, Data, Actions};

transaction_confirmed(internal, complete, 
    #transaction{direction = ?TRANSFER_DIRECTION_RECEIVE, peer = Peer} = Data) ->
    
    ok = nifi_s2s_client:write_response(Peer, ?TRANSACTION_FINISHED),

    Actions = [{next_event, internal, delete}],
    {next_state, transaction_completed, Data, Actions};    

transaction_confirmed(internal, complete, 
    #transaction{direction = ?TRANSFER_DIRECTION_SEND, peer = Peer} = Data) ->

    case nifi_s2s_client:read_response(Peer) of
        'TRANSACTION_FINISHED' ->
            Actions = [{next_event, internal, delete}],
            {next_state, transaction_completed, Data, Actions}
    end.


transaction_completed(internal, delete, #transaction{from = From}) ->
    {stop_and_reply, normal, {reply, From, ok}}.


transaction_canceled(_EventType, _EventContent, _Data) ->
    keep_state_and_data.


transaction_closed(_EventType, _EventContent, _Data) ->
    keep_state_and_data.


transaction_error(_EventType, _EventContent, _Data) ->
    keep_state_and_data.


%
% Helper functions
%

do_receive(#transaction{data_available = false} = Data) ->
    {Data, eof};

do_receive(#transaction{data_available = true, peer = Peer, crc = Crc} = Data) ->
    {Crc0, Attributes} = read_attributes(Peer, Crc),
    {Crc1, Len, Payload} = read_payload(Peer, Crc0),

    Packet = #data_packet{payload = Payload, size = Len, attributes = Attributes},

    {Data#transaction{total_transfers = Data#transaction.total_transfers + 1,
                      current_transfers = Data#transaction.current_transfers  + 1,
                      bytes = Data#transaction.bytes + Len,
                      crc = Crc1}, Packet}.


do_send(#transaction{
        peer = Peer,
        current_transfers = Ct,
        total_transfers = Tt,
        bytes = Bytes,
        crc = Crc0} = Data, Packet, Flowfile) ->

    {Crc1, Attributes} = write_attributes(Crc0, Packet),

    {Crc2, LenP, Payload} =
    case nifi_flowfile:get_content(Flowfile) of
        {0 = Len, <<>> = Content} ->
            write_payload(Crc1, Len, Content);
        {Len, Content} ->
            write_payload(Crc1, Len, Content);
        undefined ->
            write_payload(Crc1, Packet)
    end,

    ok = nifi_s2s_peer:write(Peer, [Attributes | Payload]),

    Data#transaction{current_transfers = Ct + 1,
      total_transfers = Tt + 1,
      bytes = Bytes + LenP,
      crc = Crc2}.


read_attributes(Peer, Crc) ->
    {ok, <<Size:32/integer-big>> = SizeBin} = nifi_s2s_peer:read(Peer, 4),

    Crc1 = erlang:crc32(Crc, SizeBin),

    Fun =
    fun (_N, {CrcIn, AttIn}) ->
        {Key, KeyBin} = nifi_s2s_peer:read_utf(Peer, true, true),
        {Value, ValueBin} = nifi_s2s_peer:read_utf(Peer, true, true),
       
        Crc0 = erlang:crc32(CrcIn, KeyBin),
        {erlang:crc32(Crc0, ValueBin), maps:put(Key, Value, AttIn)}
    end,

    lists:foldl(Fun, {Crc1, #{}}, lists:seq(0, Size - 1)).


write_attributes(Crc, #data_packet{attributes = Attributes}) ->
    Fun = fun(Key, Value, AccIn) ->
        KLen = byte_size(Key),
        VLen = byte_size(Value),
        % widen, KLen and VLen
        K = <<KLen:32/integer-big, Key/binary>>,
        V = <<VLen:32/integer-big, Value/binary>>,
        [ K,V | AccIn ]
    end,
    ASize = maps:size(Attributes),
    Size = <<ASize:32/integer-big>>,
    IoData =
    case ASize of
        0 ->
            Size;
        _ ->
            Properties = maps:fold(Fun, [], Attributes),
            [Size, Properties]
    end,
    {erlang:crc32(Crc, IoData), IoData}.


read_payload(Peer, Crc) ->
    {ok, <<Size:64/integer-big>> = SizeBin} = nifi_s2s_peer:read(Peer, 8),
    case Size of
        0 ->
            {erlang:crc32(Crc, SizeBin), 0, <<>>};
        _ ->
            {ok, Payload} = nifi_s2s_peer:read(Peer, Size),
            Crc1 = erlang:crc32(Crc, SizeBin),
            Crc2 = erlang:crc32(Crc1, Payload),
            {Crc2, Size, Payload}        
    end.


write_payload(Crc, #data_packet{payload = Payload}) ->
    write_payload(Crc, byte_size(Payload), Payload).

write_payload(Crc, 0 = Len, _Payload) ->
    Size = <<Len:64/integer-big>>,
    Crc1 = erlang:crc32(Crc, Size),
    IoData = [Size],
    {Crc1, Len, IoData};

write_payload(Crc, Len, Payload) ->
    Size = <<Len:64/integer-big>>,
    Crc1 = erlang:crc32(Crc, Size),
    Crc2 = erlang:crc32(Crc1, Payload),
    IoData = [Size, Payload],
    {Crc2, Len, IoData}.