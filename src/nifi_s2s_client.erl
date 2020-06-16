-module(nifi_s2s_client).

-export([new/2, decides_which_peer/1]).

-export([writeResponse/2, writeResponse/3, readResponse/1]).

-include("nifi_s2s.hrl").

new(PortId, Peer) ->
    [CurrentVersion | _ ] = ?SUPPORTED_VERSION,
    [CurrentCodecVersion | _ ] = ?SUPPORTED_CODEC_VERSION,

    #client{
        port_id = PortId,
        peer = Peer,
        running = false,
        currentVersion = CurrentVersion,
        currentCodecVersion = CurrentCodecVersion
    }.


-spec decides_which_peer(list(peer_status())) -> {ok, peer()} | {error, no_peer}.

decides_which_peer([]) ->
    {error, no_peer};

decides_which_peer([Peer | _Peers]) ->
    ct:pal("ppppp ~p", [Peer]),
    {ok, Peer}.


writeResponse(Peer, Code) ->
   nifi_s2s_peer:write(Peer, <<$R, $C, Code:8/integer>>).


writeResponse(Peer, Code, Description)  when byte_size(Description) =< 65535 ->
   Len = byte_size(Description),
   nifi_s2s_peer:write(Peer, <<$R, $C, Code:8/integer, Len:16/integer-big, Description/binary>>).


readResponse(Peer) ->
   {ok, Packet} = nifi_s2s_peer:read(Peer, 3),
   decode_response_code(Peer, Packet).


decode_response_code(Peer, <<?CODE_SEQUENCE_VALUE_1, ?CODE_SEQUENCE_VALUE_2,
                     Code:8/integer>>) ->
    case decode_response_code(Code) of
        {ACode, true} ->
            % read context message
            {ACode, nifi_s2s_peer:read_utf(Peer)};
        ACode ->
            ACode
    end.


decode_response_code(?CONTINUE_TRANSACTION) ->
    {'CONTINUE_TRANSACTION', true};

decode_response_code(?FINISH_TRANSACTION) ->
    'FINISH_TRANSACTION';

% "Explanation" of this code is the checksum
decode_response_code(?CONFIRM_TRANSACTION) ->
    {'CONFIRM_TRANSACTION', true};

decode_response_code(?TRANSACTION_FINISHED) ->
    'TRANSACTION_FINISHED';

decode_response_code(?TRANSACTION_FINISHED_BUT_DESTINATION_FULL) ->
    'TRANSACTION_FINISHED_BUT_DESTINATION_FULL';

decode_response_code(?CANCEL_TRANSACTION) ->
    {'CANCEL_TRANSACTION', true};

decode_response_code(?BAD_CHECKSUM) ->
    'BAD_CHECKSUM'.