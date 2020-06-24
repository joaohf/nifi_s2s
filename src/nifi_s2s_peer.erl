%%%-------------------------------------------------------------------
%% @doc Peer client functions.
%% @end
%% @private
%%%-------------------------------------------------------------------
-module(nifi_s2s_peer).

-export([new/2,
         new/3,
         open/1,
         close/1,
         write_utf/2,
         write/2,
         read_utf/1,
         read_utf/2,
         read_utf/3,
         read/2,
         get_url/1]).

-include("nifi_s2s.hrl").

-define(MAGIC_BYTES, <<"NiFi">>).


%%%-------------------------------------------------------------------
%% @doc Creates a new peer.
%% @end
%%%-------------------------------------------------------------------

-spec new(peer(), string()) -> s2s_peer().

new(#peer{host = Host, port = Port}, Ifc) ->
    new(Host, Port, Ifc).


%%%-------------------------------------------------------------------
%% @doc Creates a new peer based on `Host', `Port' and `Ifc'.
%% @end
%%%-------------------------------------------------------------------

-spec new(string(), non_neg_integer(), string()) -> s2s_peer().
new(Host, Port, Ifc) ->
    %ifc = get_network_interface()
    Ifc0 = Ifc,
    Url = "nifi://" ++ Host ++ ":" ++ integer_to_list(Port),

    #s2s_peer{host = Host,
     port = Port,
     local_network_interface = Ifc0,
    url = Url,
     timeout = 30000, yield_expiration = 0 }.


%%%-------------------------------------------------------------------
%% @doc Open the initial connection to the `Peer'.
%% @end
%%%-------------------------------------------------------------------

-spec open(s2s_peer()) -> {ok, s2s_peer()}.

open(#s2s_peer{host = Host, port = Port} = Peer) ->
    Sndsize = 256 * 1024,

    SocketOpts = [binary, {active, once}, {nodelay, true}, {sndbuf, Sndsize}],
    case gen_tcp:connect(Host, Port, SocketOpts) of
       {ok, Socket} ->
           ok = write(Socket, ?MAGIC_BYTES),
           {ok, Peer#s2s_peer{stream = Socket}};
       Error ->
           Error
    end. 


get_url(#s2s_peer{url = Url}) ->
    Url.


%%%-------------------------------------------------------------------
%% @doc Closes a peer conection.
%% @end
%%%-------------------------------------------------------------------

-spec close(s2s_peer()) -> s2s_peer().

close(#s2s_peer{stream = Socket} = Peer) ->
   ok = gen_tcp:close(Socket),

   Peer#s2s_peer{stream = undefined}.


-spec write_utf(s2s_peer(), iodata()) -> ok.

write_utf(#s2s_peer{} = Peer, String) when is_list(String) ->
    write_utf(Peer, list_to_binary(String));

write_utf(#s2s_peer{stream = Socket}, String) when byte_size(String) =< 65535 ->
    Len = byte_size(String),
    write(Socket, <<Len:16/integer-big, String/binary>>);

write_utf(#s2s_peer{}, String) when byte_size(String) ->
    {error, invalid_size}.


-spec write(gen_tcp:socket(), iodata()) -> ok.

write(#s2s_peer{stream = Socket}, Packet) ->
    write(Socket, Packet);

write(Socket, Packet) ->
    gen_tcp:send(Socket, Packet).


-spec read_utf(s2s_peer()) -> iodata().

read_utf(#s2s_peer{stream = Socket}) ->
    read_utf(#s2s_peer{stream = Socket}, false).

read_utf(#s2s_peer{stream = Socket}, false) ->
    {ok, <<Len:16/integer-big>>} = gen_tcp:recv(Socket, 2),
    case Len of
        0 ->
            <<>>;
        _ ->
            {ok, String} = gen_tcp:recv(Socket, Len),
            String
    end;

read_utf(#s2s_peer{stream = Socket}, true) ->
    {ok, <<Len:32/integer-big>>} = gen_tcp:recv(Socket, 4),
    {ok, String} = gen_tcp:recv(Socket, Len),
    String.

read_utf(#s2s_peer{stream = Socket}, true, true) ->
    {ok, <<Len:32/integer-big>>} = gen_tcp:recv(Socket, 4),
    {ok, String} = gen_tcp:recv(Socket, Len),
    {String, <<Len:32/integer-big, String/binary>>}.


-spec read(s2s_peer(), non_neg_integer()) -> {ok, binary()} | {error, term()}.

read(#s2s_peer{stream = Socket}, Size) ->
    gen_tcp:recv(Socket, Size).