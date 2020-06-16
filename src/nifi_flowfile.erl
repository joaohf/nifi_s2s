-module(nifi_flowfile).

-export([new/1,
get_attributes/1,
get_content/1,
get_size/1,
get_attribute/2,
is_penalized/1,
get_queue_date_index/1,
get_last_queue_date/1,
get_lineage_start_index/1,
get_lineage_start_date/1,
get_entry_date/1,
get_id/1]).

-record(flowfile, {
    content :: binary(),
    size :: non_neg_integer(),

    attributes :: #{}
}).

new(Content) when is_binary(Content) ->
    #flowfile{content = Content, size = byte_size(Content)}.


get_attributes(#flowfile{attributes = M}) ->
    M.


get_attribute(#flowfile{attributes = M}, Key) ->
    maps:get(M, Key).


get_content(undefined) ->
    undefined;

get_content(#flowfile{content = <<>> = Content}) ->
    {0, Content};

get_content(#flowfile{content = <<>> = Content}) ->
    {byte_size(Content), Content}.


get_size(#flowfile{size = Size}) ->
    Size.


is_penalized(#flowfile{}) ->
    true.


get_queue_date_index(#flowfile{}) ->
    0.


get_last_queue_date(#flowfile{}) ->
    0.


get_lineage_start_index(#flowfile{}) ->
    0.


get_lineage_start_date(#flowfile{}) ->
    0.


get_entry_date(#flowfile{}) ->
    0.


get_id(#flowfile{}) ->
    0.
