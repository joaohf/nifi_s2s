%%%-------------------------------------------------------------------
%%% File    : nifi_s2s_SUITE.erl
%%% Author  : João Henrique Ferreira de Freitas
%%% Description : A ct test
%%%
%%% Created : 2020-06-06T20:03:02+00:00
%%%-------------------------------------------------------------------
-module(nifi_s2s_SUITE).

%% Note: This directive should only be used in test suites.
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").


%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

groups() ->
    [
        {raw, [], [
            connect_disconnect,
            transfer_payload,
            transfer_flowfiles,
            receive_flowfiles
        ]}
    ].

all() ->
    [
        {group, raw}
    ].


suite() ->
    [{timetrap, {seconds, 45}}].


init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(jsx),

    % output_port and input_port are nifi port processors
    [{output_port, "c62bad66-0172-1000-a957-7ec79ed4f525"},
     {input_port, "8f7630f3-0172-1000-8f82-0a81a44f3d30"} | Config].


end_per_suite(_Config) ->
    ok.


init_per_group(_GroupName, Config) ->
    Config.


end_per_group(_GroupName, _Config) ->
    ok.


init_per_testcase(_TestCase, Config) ->
    Config.


end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

connect_disconnect() ->
    [].

connect_disconnect(Config) ->
    S2SConfig = #{hostname => "localhost",
                  port => 8080,
                  transport_protocol => raw,
                  local_network_interface => "lo0",
                  port_id => ?config(input_port, Config)},

    {ok, _Pid} = nifi_s2s:create_client(S2SConfig),

    ct:sleep(5000),

    ok.


transfer_payload() ->
    [].

transfer_payload(Config) ->
    S2SConfig = #{hostname => "localhost",
                  port => 8080,
                  transport_protocol => raw,
                  local_network_interface => "lo0",
                  port_id => ?config(input_port, Config)},

    {ok, Pid} = nifi_s2s:create_client(S2SConfig),

    Payload = <<"Test Nifi Payload">>,
    Attributes = #{ <<"TEST1">> => <<"Test">>},

    ok = nifi_s2s:transmit_payload(Pid, Payload, Attributes),

    ok = nifi_s2s:close(Pid),

    ok.


transfer_flowfiles() ->
    [].

transfer_flowfiles(Config) ->
    S2SConfig = #{hostname => "localhost",
                  port => 8080,
                  transport_protocol => raw,
                  local_network_interface => "lo0",
                  port_id => ?config(input_port, Config)},

    {ok, Pid} = nifi_s2s:create_client(S2SConfig),

    Content = <<"Test Nifi Content">>,
    Attributes = #{ <<"TEST1">> => <<"Test">>},

    Fun = fun(_Elem, AccIn) -> nifi_flowfile:add(Attributes, Content, AccIn) end,
    Flowfiles = lists:foldl(Fun, nifi_flowfile:new(), lists:seq(0, 10)),

    ok = nifi_s2s:transfer_flowfiles(Pid, Flowfiles),

    ok = nifi_s2s:close(Pid),

    ok.


receive_flowfiles() ->
    [].

receive_flowfiles(Config) ->
    S2SConfig = #{hostname => "localhost",
                  port => 8080,
                  transport_protocol => raw,
                  local_network_interface => "lo0",
                  port_id => ?config(output_port, Config)},

    {ok, Pid} = nifi_s2s:create_client(S2SConfig),

    {ok, _Flowfiles} = nifi_s2s:receive_flowfiles(Pid),

    ok = nifi_s2s:close(Pid),

    ok.

%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------

