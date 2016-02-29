-module(poolboy).
-export([
	checkout/1, 
	checkout/2, 
	checkin/2, 
	transaction/2, 
	transaction/3, 
	child_spec/3, 
	stop/1, 
	status/1,
	dismiss/2,
	change_pool_size/2
]).

-define(TIMEOUT, 5000).


checkout(Pool) ->
    checkout(Pool, ?TIMEOUT).
checkout(Pool, Timeout) ->
    gen_server:call(Pool, {checkout, Timeout}, Timeout).
	

checkin(Pool, Worker) when is_pid(Worker) ->
    gen_server:cast(Pool, {checkin, self(), Worker}).


dismiss(Pool, Worker) when is_pid(Worker) ->
	gen_server:cast(Pool, {stop_worker, Worker}).


change_pool_size(Pool, NewSize) when is_integer(NewSize) ->
	gen_server:call(Pool, {change_pool_size, NewSize}).
	

transaction(Pool, Fun) ->
    transaction(Pool, Fun, ?TIMEOUT).
transaction(Pool, Fun, Timeout) ->
    Worker   = poolboy:checkout(Pool, Timeout),
	try
		Fun(Worker)
	after
		ok = poolboy:checkin(Pool, Worker)
	end.


child_spec(Pool, PoolArgs, WorkerArgs) ->
    {Pool, {poolboy_server, start_link, [PoolArgs, WorkerArgs]}, permanent, 5000, worker, [poolboy]}.


stop(Pool) ->
    gen_server:call(Pool, stop).


status(Pool) ->
    gen_server:call(Pool, status).

