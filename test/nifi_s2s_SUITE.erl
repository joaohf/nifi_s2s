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
    Config.


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

connect_disconnect(_Config) ->
    S2SConfig = #{host => "localhost",
     port => 9001,
     client_type => raw, portId => "8f7630f3-0172-1000-8f82-0a81a44f3d30"},

    {ok, _Pid} = nifi_s2s:create_client(S2SConfig),

    ct:sleep(5000),

    ok.


transfer_payload() ->
    [].

transfer_payload(_Config) ->
    S2SConfig = #{host => "localhost",
     port => 9001,
     client_type => raw, portId => "8f7630f3-0172-1000-8f82-0a81a44f3d30"},

    {ok, Pid} = nifi_s2s:create_client(S2SConfig),

    Payload = <<"Test Nifi Payload">>,
    %Attributes = #{ <<"TEST1">> => <<"Test">>},
    Attributes = #{},
    ok = nifi_s2s:transmit_payload(Pid, Payload, Attributes),

    ok = nifi_s2s:close(Pid),

    ok.


transfer_flowfiles() ->
    [].

transfer_flowfiles(_Config) ->
    S2SConfig = #{host => "localhost",
     port => 9001,
     client_type => raw,
     portId => "8f7630f3-0172-1000-8f82-0a81a44f3d30"},

    {ok, Pid} = nifi_s2s:create_client(S2SConfig),

    Content = <<"Test Nifi Content">>,
    Attributes = #{ <<"TEST1">> => <<"Test">>},

    Flowfile = nifi_flowfile:new(Attributes, Content),

    ok = nifi_s2s:transfer_flowfiles(Pid, Flowfile),

    ok = nifi_s2s:close(Pid),

    ok.


receive_flowfiles() ->
    [].

receive_flowfiles(_Config) ->
    S2SConfig = #{host => "localhost",
     port => 9001,
     client_type => raw,
     portId => "c62bad66-0172-1000-a957-7ec79ed4f525"},

    {ok, Pid} = nifi_s2s:create_client(S2SConfig),

    {ok, _Flowfiles} = nifi_s2s:receive_flowfiles(Pid),

    ok = nifi_s2s:close(Pid),

    ok.

%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------

