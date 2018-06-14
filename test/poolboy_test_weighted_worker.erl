-module(poolboy_test_weighted_worker).
-behaviour(gen_server).
-behaviour(poolboy_weighted_worker).

-export([start_link/1]).
-export([init/1, get_weight/1, set_weight/2, die/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Weight) ->
    gen_server:start_link(?MODULE, #{weight => Weight}, []).

init(State) ->
    {ok, State}.

get_weight(Pid) ->
    gen_server:call(Pid, get_weight).

set_weight(Pid, Weight) ->
    gen_server:call(Pid, {set_weight, Weight}).

die(Pid) ->
    gen_server:call(Pid, die).

handle_call(get_weight, _From, #{weight := Weight} = State) ->
    {reply, Weight, State};
handle_call({set_weight, Weight}, _From, State) ->
    NewState = maps:put(weight, Weight, State),
    {reply, ok, NewState};
handle_call(die, _From, State) ->
    {stop, {error, died}, dead, State};
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
