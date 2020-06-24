%%%-------------------------------------------------------------------
%% @doc NiFi Flowfile definition and helper functions.
%%      Flowfile are simple messages with an attributes part and an
%%      optional payload.
%% @end
%%%-------------------------------------------------------------------
-module(nifi_flowfile).

-export([new/0, new/1, new/2,
         add/2, add/3,
         remove/1,
         get_attributes/1,
         get_content/1,
         get_size/1,
         get_attribute/2]).

-include("nifi_s2s.hrl").

-record(flowfile, {
    content :: binary(),
    size :: non_neg_integer(),

    attributes :: #{}
}).
-opaque flowfile() :: #flowfile{}.
%% A record to hold flowfile definition.

-opaque flowfiles() :: queue:queue().
%% Holds a set of flowfiles received or to be send.

-export_type([flowfile/0, flowfiles/0]).


%%%-------------------------------------------------------------------
%% @doc Creates a new object to hold flowfiles.
%% @end
%%%-------------------------------------------------------------------
-spec new() -> flowfiles().

new() ->
    queue:new().

%%%-------------------------------------------------------------------
%% @private
%%%-------------------------------------------------------------------
new([]) ->
    [];


%%%-------------------------------------------------------------------
%% @private
%%%-------------------------------------------------------------------

new(Packets) when is_list(Packets)->
    [ new(Packet#data_packet.attributes, Packet#data_packet.payload) || Packet <- Packets ];


%%%-------------------------------------------------------------------
%% @private
%%%-------------------------------------------------------------------

new(Content) ->
    new(#{}, Content).


%%%-------------------------------------------------------------------
%% @doc Creates a new flowfile with `Attributes' and `Content'.
%% @end
%%%-------------------------------------------------------------------

-spec new(map(), binary()) -> flowfile().

new(Attributes, Content) when is_map(Attributes), is_binary(Content) ->
    #flowfile{content = Content, size = byte_size(Content), attributes = Attributes}.


%%%-------------------------------------------------------------------
%% @doc Adds the `Flowfile' to `Flowfiles'.
%% @end
%%%-------------------------------------------------------------------

-spec add(flowfile(), flowfiles()) -> flowfiles().

add(Flowfile, Flowfiles) ->
    queue:in(Flowfile, Flowfiles).


%%%-------------------------------------------------------------------
%% @doc Create a new flowfile and add it to `Flowfiles'.
%% @end
%%%-------------------------------------------------------------------

-spec add(map(), binary(), flowfiles()) -> flowfiles().

add(Attributes, Content, Flowfiles) ->
    add(new(Attributes, Content), Flowfiles).


%%%-------------------------------------------------------------------
%% @doc Remove and returns a flowfile from `Flowfile'.
%% @end
%%%-------------------------------------------------------------------

-spec remove(flowfiles()) -> {{value, flowfile()}, flowfiles()} | {empty, flowfiles()}.

remove(Flowfiles) ->
    queue:out(Flowfiles).


-spec get_attributes(flowfile()) -> map().

get_attributes(#flowfile{attributes = M}) ->
    M.


-spec get_attribute(flowfile(), binary()) -> binary().

get_attribute(#flowfile{attributes = M}, Key) ->
    maps:get(M, Key).


-spec get_content(flowfile()) -> binary().

get_content(undefined) ->
    undefined;

get_content(#flowfile{content = <<>> = Content}) ->
    {0, Content};

get_content(#flowfile{content = Content}) ->
    {byte_size(Content), Content}.


-spec get_size(flowfile()) -> non_neg_integer().

get_size(#flowfile{size = Size}) ->
    Size.