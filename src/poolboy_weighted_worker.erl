%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy_weighted_worker).

-callback start_link(WorkerArgs) -> {ok, Pid} |
                                    {error, {already_started, Pid}} |
                                    {error, Reason} when
    WorkerArgs :: proplists:proplist(),
    Pid        :: pid(),
    Reason     :: term().

-callback get_weight(WorkerPid) -> Weight  when
    Weight :: pos_integer() | full,
    WorkerPid :: pid().
