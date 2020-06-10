-ifndef(nifi_s2s).
-define(nifi_s2s, true).


-record(peer, {
    host :: string(),
    port :: non_neg_integer(),
    port_id :: any(),
    secure :: boolean(),

    status :: peer_status()
}).

-record(peer_status, {
    flow_file_count :: non_neg_integer(),
    query_for_peers :: boolean()
}).

-record(s2s_peer, {
    host :: string(),
    port :: non_neg_integer(),
    local_network_interface :: string(),

    % URL
    url :: string(),
    % socket timeout
    timeout :: non_neg_integer(),
    % Yield Period in Milliseconds
    yield_period_msec :: non_neg_integer(),
    % Yield Expiration
    yield_expiration :: non_neg_integer(),
    % Yield Expiration per destination PortID
    yield_expiration_PortIdMap :: non_neg_integer(),


    % ?? data stream
    stream :: any()
}).

-record(client, {
    % portIDStr
    port_id_str :: string(),
    % portId
    port_id :: any(),
    % idleTimeout
    idle_timeout :: non_neg_integer(),
    % Peer Connection
    peer :: s2s_peer(),

    % running
    running :: boolean(),

    % transaction map
    known_transactions :: #{},

    % batch send nanos
    batchSendNanos :: non_neg_integer(),

    % versioning
    %supportedVersion =  :: list(non_neg_integer()),
    currentVersion :: non_neg_integer(),
    %supportedCodecVersion = [1] :: list(non_neg_integer()),
    currentCodecVersion :: non_neg_integer()
%    currentCodecVersionIndex :: non_neg_integer()
}).

-record(configuration, {
    stream,
    peer :: s2s_peer(),
    client_type :: nifi_s2s:client_type(),
    local_network_interface :: string(),
    idle_timeout :: non_neg_integer()
}).

-type configuration() :: #configuration{}.
-type peer() :: #peer{}.
-type peer_status() :: #peer_status{}.
-type s2s_peer() :: #s2s_peer{}.
-type client() :: #client{}.


-define(SUPPORTED_VERSION, [5, 4, 3, 2, 1]).
-define(SUPPORTED_CODEC_VERSION, [1]).

%
% Resource Negotiated Status Code
%
-define(RESOURCE_OK, 20).
-define(DIFFERENT_RESOURCE_VERSION, 21).
-define(NEGOTIATED_ABORT, 255).

%
% Respond Code
%

% ResponseCode, so that we can indicate a 0 followed by some other bytes
-define(RESERVED, 0).

% handshaking properties
-define(PROPERTIES_OK, 1).
-define(UNKNOWN_PROPERTY_NAME, 230).
-define(ILLEGAL_PROPERTY_VALUE, 231).
-define(MISSING_PROPERTY, 232).

% transaction indicators
-define(CONTINUE_TRANSACTION, 10).
-define(FINISH_TRANSACTION, 11).
-define(CONFIRM_TRANSACTION, 12). % "Explanation" of this code is the checksum
-define(TRANSACTION_FINISHED, 13).
-define(TRANSACTION_FINISHED_BUT_DESTINATION_FULL, 14).
-define(CANCEL_TRANSACTION, 15).
-define(BAD_CHECKSUM, 19).

% data availability indicators
-define(MORE_DATA, 20).
-define(NO_MORE_DATA, 21).

% port state indicators
-define(UNKNOWN_PORT, 200).
-define(PORT_NOT_IN_VALID_STATE, 201).
-define(PORTS_DESTINATION_FULL, 202).

% authorization
-define(UNAUTHORIZED, 240).

% error indicators
-define(ABORT, 250).
-define(UNRECOGNIZED_RESPONSE_CODE, 254).
-define(END_OF_STREAM, 255).



%// Request Type
%typedef enum {
%  NEGOTIATE_FLOWFILE_CODEC = 0,
%  REQUEST_PEER_LIST,
%  SEND_FLOWFILES,
%  RECEIVE_FLOWFILES,
%  SHUTDOWN,
%  MAX_REQUEST_TYPE
%} RequestType;

%const char *SiteToSiteRequest::RequestTypeStr[MAX_REQUEST_TYPE] = {
%    "NEGOTIATE_FLOWFILE_CODEC",
-define(REQUEST_PEER_LIST, <<"REQUEST_PEER_LIST">>).
%    "SEND_FLOWFILES",
%    "RECEIVE_FLOWFILES", 
-define(SHUTDOWN, <<"SHUTDOWN">>).

-endif.