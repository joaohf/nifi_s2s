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
            connect_disconnect
        ]}
    ].

all() ->
    [
        {group, raw}
    ].


suite() ->
    [{timetrap, {seconds, 15}}].


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
    S2SConfig = #{host => "localhost", port => 9001, client_type => raw},

    {ok, _Pid} = nifi_s2s:create_client(S2SConfig),

    ok.

%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------

