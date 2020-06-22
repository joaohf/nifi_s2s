% https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#site-to-site-protocol-sequence
-module(nifi_s2s).

-include("nifi_s2s.hrl").

-type scheme() :: string().
-type transport_protocol() :: raw | http.

-type s2s_config() :: #{scheme => scheme(),
                         hostname => string(),
                         port => non_neg_integer(),
                         transport_protocol => transport_protocol(),
                         local_network_interface => string(),
                         direction => 'send' | 'receive'}.

-export_type([s2s_config/0, transport_protocol/0]).

-export([create_client/1,
         close/1,
         transmit_payload/3,
         transfer_flowfiles/2,
         receive_flowfiles/1]).


create_raw_socket(#{host := Host, port := Port, port_id := PortId, local_network_interface := Ifc}) ->
    Peer = nifi_s2s_peer:new(Host, Port, Ifc),

    Client = nifi_s2s_client:new(PortId, Peer),

    % 3. The client sends another request to get remote peers using the
    % TCP port number returned at #2.
    {ok, Pid} = nifi_s2s_raw_protocol_statem:start_link(Client, peer_list),

    % 4. A remote NiFi node responds with list of available remote peers
    % containing hostname, port, secure and workload such as the number of queued FlowFiles
    {ok, Peers} = nifi_s2s_raw_protocol_statem:get_peer_list(Pid),

    % From this point, further communication is done between the client and
    % the remote NiFi node.

    % 5. The client decides which peer to transfer data from/to,
    % based on workload information.
    {ok, NewPeer}  = nifi_s2s_client:decides_which_peer(Peers),

    NClient = nifi_s2s_client:new(PortId, nifi_s2s_peer:new(NewPeer, Ifc)),

    {ok, NClient}.
    

create_client(#{transport_protocol := raw = TransportProtocol} = S2SConfig) ->
    {ok, #{host := PeerHost, port := PeerPort, secure := Secure}} = refresh_peer_list(S2SConfig),

    PortId = maps:get(port_id, S2SConfig),
    Ifc = maps:get(local_network_interface, S2SConfig),
    NS2SConfig = #{host => PeerHost,
                  port => PeerPort,
                  secure => Secure,
                  transport_protocol => TransportProtocol,
                  local_network_interface => Ifc,
                  port_id => PortId},

    {ok, Peer} = create_raw_socket(NS2SConfig),

    % 6. Connect to remote Peer
    nifi_s2s_raw_protocol_statem:start_link(Peer, peer).


close(Pid) ->
    nifi_s2s_raw_protocol_statem:stop(Pid).


% 1. A client initiates Site-to-Site protocol by sending a HTTP(S) request to the
% specified remote URL to get remote cluster Site-to-Site information.
% Specifically, to '/nifi-api/site-to-site'. This request is called SiteToSiteDetail.
% 2. A remote NiFi node responds with its input and output ports, and TCP port numbers
% for RAW and TCP transport protocols.
get_site_to_site_detail(Url) ->
    UriMap = uri_string:parse(Url),
    WithPath = maps:put(path, "/nifi-api/site-to-site", UriMap),

    FullUrl = uri_string:recompose(WithPath),

    Response = httpc:request(get, {FullUrl, []}, [], [{body_format, binary}]),
    {ok, Port, Secure} = parse_detail(Response),

    Host = maps:get(host, UriMap),

    {ok, #{host => Host, port => Port, secure => Secure}}.


parse_detail({ok, {{_, 200, _}, Headers, Body}}) ->
    ContentType = proplists:get_value("content-type", Headers),
    ContentLength = proplists:get_value("content-length", Headers),

    case {ContentType, ContentLength} of
        {"application/json", Cl} when Cl > 0 ->
            make_site_to_site_config(jsx:decode(Body));
        {_, _} ->
            erlang:error(invalid_site_to_site)
    end.

make_site_to_site_config(List) ->
    Controller = proplists:get_value(<<"controller">>, List, []),
    Port = proplists:get_value(<<"remoteSiteListeningPort">>, Controller),
    Secure = proplists:get_value(<<"siteToSiteSecure">>, Controller),

    {ok, Port, Secure}.


refresh_peer_list(S2SConfig) ->
    Url = make_url(S2SConfig),
    get_site_to_site_detail(Url).


make_url(#{scheme := Scheme, hostname := Host, port := Port}) ->
    lists:flatten([Scheme, "://", Host, ":", integer_to_list(Port)]);

make_url(#{hostname := _Host, port := _Port} = M) ->
    make_url(maps:put(scheme, "http", M)).


%% @doc Transfers flow file to server.
%% @end
transfer_flowfiles(Pid, Flowfile) ->
    {ok, Transaction} = 
        nifi_s2s_raw_protocol_statem:transfer_flowfile(Pid, Flowfile),

    ok = nifi_s2s_transaction_statem:confirm(Transaction).


%% @doc Receive flow file from server.
%% @end
receive_flowfiles(Pid) ->
    {ok, _FlowFile} = nifi_s2s_raw_protocol_statem:receive_flowfiles(Pid).


%% @doc Transfers raw data and attributes to server.
%% @end

-spec transmit_payload(Pid :: pid(), Payload :: binary(), Attributes :: map()) -> boolean().

transmit_payload(Pid, Payload, Attributes) ->
    nifi_s2s_raw_protocol_statem:transmit_payload(Pid, Payload, Attributes).
    