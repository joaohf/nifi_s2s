% https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#site-to-site-protocol-sequence
-module(nifi_s2s).

-include("nifi_s2s.hrl").

-type client_type() :: raw.

-export_type([client_type/0]).

-export([get_site_to_site_detail/1, create_client/1]).

create_raw_socket(#{host := Host, port := Port}) ->
    Ifc = "lo0",

    Peer = nifi_s2s_peer:new(Host, Port, "lo0"),

    Client = nifi_s2s_client:new(Peer),

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

    NClient = nifi_s2s_client:new(nifi_s2s_peer:new(NewPeer, Ifc)),

    % 6. Connect to remote Peer
    {ok, _Pid} = nifi_s2s_raw_protocol_statem:start_link(NClient, peer).


create_client(#{client_type := raw} = S2SConfig) ->
    create_raw_socket(S2SConfig).


% 1. A client initiates Site-to-Site protocol by sending a HTTP(S) request to the
% specified remote URL to get remote cluster Site-to-Site information.
% Specifically, to '/nifi-api/site-to-site'. This request is called SiteToSiteDetail.
% 2. A remote NiFi node responds with its input and output ports, and TCP port numbers
% for RAW and TCP transport protocols.
get_site_to_site_detail(_Url) ->
    #{host => "localhost", port => 9001, client_type => raw}.