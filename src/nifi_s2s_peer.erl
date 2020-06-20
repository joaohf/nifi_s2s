%%%-------------------------------------------------------------------
%% @doc Site 2 Site client state machine.
%% @end
%%%-------------------------------------------------------------------
-module(nifi_s2s_peer).

-export([new/2, new/3, open/1, close/1,
    write_utf/2, write/2,
    read_utf/1, read_utf/2, read_utf/3,
    read/2,
    get_url/1]).

-include("nifi_s2s.hrl").

-define(MAGIC_BYTES, <<"NiFi">>).

%// setHostName
%  void setHostName(std::string host_) {
%    this->host_ = host_;
%    url_ = "nifi://" + host_ + ":" + std::to_string(port_);
%  }
%  // setPort
%  void setPort(uint16_t port_) {
%    this->port_ = port_;
%    url_ = "nifi://" + host_ + ":" + std::to_string(port_);
%  }

new(#peer{host = Host, port = Port}, Ifc) ->
    new(Host, Port, Ifc).


new(Host, Port, Ifc) ->
    %ifc = get_network_interface()
    Ifc0 = Ifc,
    Url = "nifi://" ++ Host ++ ":" ++ integer_to_list(Port),

    #s2s_peer{host = Host,
     port = Port,
     local_network_interface = Ifc0,
    url = Url,
     timeout = 30000, yield_expiration = 0 }.


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


close(#s2s_peer{stream = Socket} = Peer) ->
   ok = gen_tcp:close(Socket),

   Peer#s2s_peer{stream = undefined}.

write_utf(#s2s_peer{} = Peer, String) when is_list(String) ->
    write_utf(Peer, list_to_binary(String));

write_utf(#s2s_peer{stream = Socket}, String) when byte_size(String) =< 65535 ->
    Len = byte_size(String),
    write(Socket, <<Len:16/integer-big, String/binary>>);

write_utf(#s2s_peer{}, String) when byte_size(String) ->
    {error, invalid_size}.

write(#s2s_peer{stream = Socket}, Packet) ->
    write(Socket, Packet);

write(Socket, Packet) ->
    gen_tcp:send(Socket, Packet).


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


read(#s2s_peer{stream = Socket}, Size) ->
    gen_tcp:recv(Socket, Size).