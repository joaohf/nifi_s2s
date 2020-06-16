-module(nifi_s2s_transaction_statem).

-behaviour(gen_statem).

-export([create/1, send/3, confirm/1, delete/1]).
-export([callback_mode/0, init/1, terminate/3]).
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
      
      %bool closed_;

      % Whether received data is available
      dataAvailable :: boolean(),

 
      % org::apache::nifi::minifi::io::CRCStream<SiteToSitePeer> crcStream;

      %  // Transaction Direction TransferDirection
      direction :: 'send' | 'receive',

      % A global unique identifier
      uuid :: any(),
      %// UUID string
      %std::string uuid_str_;

      %static std::shared_ptr<utils::IdGenerator> id_generator_;

      peer :: s2s_peer(),

      crc :: non_neg_integer(),

      from = undefined:: {To :: pid(), Tag :: term()},

      current_version :: non_neg_integer()
}).


create(Peer) ->
    TransactionId = uuid:get_v4(),
    {ok, Pid} = gen_statem:start_link(?MODULE, {Peer, TransactionId}, []),
    {ok, Pid, TransactionId}.


send(Transaction, Packet, Flowfile) ->
    gen_statem:call(Transaction, {send, Packet, Flowfile}).


confirm(Transaction) ->
    gen_statem:call(Transaction, {confirm, []}).


delete(Transaction) ->
    gen_statem:stop(Transaction).


%% gen_statem callbacks

callback_mode() -> [state_functions].

init({Peer, TransactionId}) ->
    Data = #transaction{peer = Peer, uuid = TransactionId, crc = 0},
    Actions = [{next_event, internal, new}],
    {ok, transaction_started, Data, Actions}.


terminate(_, _, _Data) ->
    ok.


transaction_started(internal, new, #transaction{}) ->
    keep_state_and_data;

transaction_started({call, From}, {send, Packet, Flowfile},
    #transaction{} = Data) ->

    NData = do_send(Data, Packet, Flowfile),

    Actions = [{reply, From, ok}],
    {next_state, data_exchanged, NData#transaction{direction = ?TRANSFER_DIRECTION_SEND}, Actions}.


data_exchanged({call, From}, {send, Packet, Flowfile},
    #transaction{direction = ?TRANSFER_DIRECTION_SEND, peer = Peer,
     current_transfers = Ct} = Data) when Ct > 0 ->
    
    ok = nifi_s2s_client:writeResponse(Peer,
        ?CONTINUE_TRANSACTION, <<"CONTINUE_TRANSACTION">>),
    
    NData = do_send(Data, Packet, Flowfile),

    Actions = [{reply, From, ok}],
    {keep_state, NData, Actions};

data_exchanged({call, From}, {confirm, []},
    #transaction{direction = ?TRANSFER_DIRECTION_SEND, peer = Peer, crc = Crc,
    current_version = Cv} = Data) ->

    CrcBin = integer_to_binary(Crc),

    ok = nifi_s2s_client:writeResponse(Peer, ?FINISH_TRANSACTION),

    % take into account the protocol version
    case nifi_s2s_client:readResponse(Peer) of
        {'CONFIRM_TRANSACTION', CrcT} when CrcT =:= CrcBin, Cv >= 3 ->
            ok = nifi_s2s_client:writeResponse(Peer,
                ?CONFIRM_TRANSACTION, <<>>),

            Actions = [{next_event, internal, complete}],
            {next_state, transaction_confirmed, Data#transaction{from = From}, Actions};

        {'CONFIRM_TRANSACTION', _C} when Cv >= 3 ->
            ok = nifi_s2s_client:writeResponse(Peer,
                ?BAD_CHECKSUM, <<"BAD_CHECKSUM">>),

            {stop, bad_checksum};

        'CONFIRM_TRANSACTION' ->
            ok = nifi_s2s_client:writeResponse(Peer,
                ?CONFIRM_TRANSACTION, <<>>),

            Actions = [{next_event, internal, complete}],
            {next_state, transaction_confirmed, Data#transaction{from = From}, Actions}
    end.


transaction_confirmed(internal, complete, 
    #transaction{direction = ?TRANSFER_DIRECTION_SEND, peer = Peer} = Data) ->

    case nifi_s2s_client:readResponse(Peer) of
        'TRANSACTION_FINISHED' ->
            Actions = [{next_event, internal, delete}],
            {next_state, transaction_completed, Data, Actions}
    end.


transaction_completed(internal, delete, #transaction{from = From}) ->
    Actions = [{reply, From, ok}],
    {stop_and_reply, delete, Actions}.


transaction_canceled(_EventType, _EventContent, _Data) ->
    keep_state_and_data.


transaction_closed(_EventType, _EventContent, _Data) ->
    keep_state_and_data.


transaction_error(_EventType, _EventContent, _Data) ->
    keep_state_and_data.


%
% Helper functions
%

do_send(#transaction{
        peer = Peer,
        current_transfers = Ct,
        total_transfers = Tt,
        bytes = Bytes,
        crc = Crc0} = Data, Packet, Flowfile) ->

    % prepare the packet and flowfile

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

    IoData = [Attributes | Payload],

    ct:pal("ss ~p", [IoData]),

    % ? update Package size ?

    ok = nifi_s2s_peer:write(Peer, IoData),

    Data#transaction{current_transfers = Ct + 1,
      total_transfers = Tt + 1,
      bytes = Bytes + LenP,
      crc = Crc2}.


write_attributes(Crc, #data_packet{attributes = Attributes}) ->
    Fun = fun(Key, Value, AccIn) ->
        KLen = byte_size(Key),
        VLen = byte_size(Value),
        K = <<KLen:16/integer-big, Key/binary>>,
        V = <<VLen:16/integer-big, Value/binary>>,
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