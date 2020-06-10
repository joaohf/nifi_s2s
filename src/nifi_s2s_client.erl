-module(nifi_s2s_client).

-export([new/1, decides_which_peer/1]).

-include("nifi_s2s.hrl").

new(Peer) ->
    [CurrentVersion | _ ] = ?SUPPORTED_VERSION,
    [CurrentCodecVersion | _ ] = ?SUPPORTED_CODEC_VERSION,
    #client{
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