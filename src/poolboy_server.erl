-module(poolboy_server).

-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {

  workers = [],
  waiting :: queue:queue(),
  monitors :: ets:tid(),


  total_size = 0,
  start_size,
  min_size,
  max_size,

  total_wait_loading = 50,
  wait_loading = 0,

  worker_timeout = 30000,
  worker_mod,
  worker_args

}).


start_link(PoolArgs, WorkerArgs) ->
  Name = proplists:get_value(name, PoolArgs),
  gen_server:start_link(Name, ?MODULE, {PoolArgs, WorkerArgs}, []).





init({PoolArgs, WorkerArgs}) ->

  process_flag(trap_exit, true),

  Monitors = ets:new(monitors, [public, set]),

  StartSize = proplists:get_value(start_size, PoolArgs, 0),
  MinSize = proplists:get_value(min_size, PoolArgs, 1),
  MaxSize = proplists:get_value(max_size, PoolArgs, 50),
  WorkerMod = proplists:get_value(worker_module, PoolArgs),

  State = #state{
    monitors = Monitors,
    waiting = queue:new(),
    start_size = StartSize,
    min_size = MinSize,
    max_size = MaxSize,
    worker_mod = WorkerMod,
    worker_args = WorkerArgs
  },

  poolboy_manager:start_workers(StartSize, WorkerMod, WorkerArgs),

  erlang:send_after(State#state.worker_timeout, self(), worker_timeout),

  {ok, State#state{wait_loading = StartSize}}.


% Убиваем worker если превышен максимальный лимит
handle_cast({add_worker, WorkerPid}, State =
  #state{
    max_size = MaxSize,
    total_size = Total,
    wait_loading = WaitLoading
  }) when Total =:= MaxSize ->

  poolboy_manager:stop_worker(WorkerPid),

  {noreply, State#state{wait_loading = WaitLoading - 1}};


% добавляем новый воркер
handle_cast({add_worker, WorkerPid}, State =
  #state{
    workers = Workers,
    total_size = Total,
    waiting = Waiting,
    monitors = Monitors,
    wait_loading = WaitLoading
  }) ->

  Ref = erlang:monitor(process, WorkerPid),
  true = ets:insert(Monitors, {WorkerPid, Ref}),

  {Waiting2, Workers2} = poolboy_manager:checkin(WorkerPid, Waiting, Workers, Monitors),

  NewState = State#state{
    workers = Workers2,
    total_size = Total + 1,
    waiting = Waiting2,
    wait_loading = WaitLoading - 1
  },

  {noreply, NewState};


%% остановить вокрер
handle_cast({stop_worker, WorkerPid}, State) ->

  #state{
    total_size = Total,
    monitors = Monitors} = State,

  NewTotal =
    case ets:lookup(Monitors, WorkerPid) of
      [{WorkerPid, Ref}] ->

        true = erlang:demonitor(Ref),
        true = ets:delete(Monitors, WorkerPid),
        poolboy_manager:stop_worker(WorkerPid),
        Total - 1;

      _ ->

        true = ets:delete(Monitors, WorkerPid),
        poolboy_manager:stop_worker(WorkerPid),
        Total

    end,

  {noreply, State#state{total_size = NewTotal}};


% не удалось запустить воркер
handle_cast(worker_crash_in_start, State) ->

  #state{wait_loading = WaitLoading} = State,

  {noreply, State#state{wait_loading = WaitLoading - 1}};


% return worker to queue
handle_cast({checkin, FromPid, WorkerPid}, State =
  #state{
    monitors = Monitors,
    workers = Workers,
    waiting = Waiting
  }) ->

  NewState =
    case ets:lookup(Monitors, FromPid) of
      [{FromPid, WorkerPid, Ref, client}] ->

        true = erlang:demonitor(Ref),
        true = ets:delete(Monitors, FromPid),

        case process_info(WorkerPid, status) of
          undefined ->
            State;

          _ ->

            {Waiting2, Workers2} =
              poolboy_manager:checkin(WorkerPid, Waiting, Workers, Monitors),

            State#state{
              workers = Workers2,
              waiting = Waiting2
            }

        end;

      [] ->
        State

    end,

  {noreply, NewState};


handle_cast(_Msg, State) ->
  {noreply, State}.


% изменить размер пула
handle_call({change_pool_size, Size}, _From, State) ->
  {reply, ok, State#state{max_size = Size}};


% get free worker
handle_call({checkout, Timeout}, {FromPid, _} = From, State) ->
  #state{
    total_size = Total,
    workers = Workers,
    monitors = Monitors,
    waiting = Waiting,
    total_wait_loading = TotalWaitLoading,
    wait_loading = WaitLoading,
    max_size = MaxSize} = State,


  case Workers of
    [] ->

      NewState =
        case Total < MaxSize of
          true ->

            case WaitLoading < TotalWaitLoading of
              true ->

                poolboy_manager:add_worker(State#state.worker_mod, State#state.worker_args),
                State#state{wait_loading = WaitLoading + 1};

              false ->
                State

            end;

          false ->
            State

        end,

      Waiting2 = to_waiting(From, Timeout, Monitors, Waiting),

      {noreply, NewState#state{waiting = Waiting2}};

    [{WorkerPid, _} | RestWorkers] ->

      Ref = erlang:monitor(process, FromPid),
      true = ets:insert(Monitors, {FromPid, WorkerPid, Ref, client}),
      {reply, WorkerPid, State#state{workers = RestWorkers}}

  end;


% get pool stats
handle_call(status, _From, State) ->
  #state{
    workers = Workers,
    waiting = Waiting,
    monitors = Monitors,
    total_size = Total,
    max_size = MaxSize,
    min_size = MinSize,
    wait_loading = WaitLoading,
    total_wait_loading = TotalWaitLoading
  } = State,
  Resp = [
    {total, Total},
    {max_size, MaxSize},
    {min_size, MinSize},
    {workers, length(Workers)},
    {waiting, queue:len(Waiting)},
    {monitors, ets:info(Monitors, size)},
    {total_wait_loading, TotalWaitLoading},
    {wait_loading, WaitLoading}
  ],
  {reply, Resp, State};





handle_call(_Msg, _From, State) ->
  Reply = {error, invalid_message},
  {reply, Reply, State}.


% clear unused workers
handle_info(worker_timeout, State =
  #state{
    total_size = Total,
    min_size = MinSize,
    worker_timeout = Timeout,
    workers = Workers,
    monitors = Monitors}) ->

  Now = os:timestamp(),

  {NewTotal, NewWorkers} =
    lists:foldl(fun({Pid, TimeLastUsed}, {TotalAcc, WorkersAcc}) ->

      Waited = timer:now_diff(Now, TimeLastUsed),
      ValidTime = ((Waited div 1000) < Timeout),

      case ValidTime of
        true ->
          {TotalAcc, [{Pid, TimeLastUsed} | WorkersAcc]};

        false ->

          case TotalAcc > MinSize of
            false ->
              {TotalAcc, [{Pid, TimeLastUsed} | WorkersAcc]};

            true ->

              case ets:lookup(Monitors, Pid) of
                [{Pid, Ref}] ->

                  true = erlang:demonitor(Ref),
                  true = ets:delete(Monitors, Pid),
                  poolboy_manager:stop_worker(Pid),
                  {TotalAcc - 1, WorkersAcc};

                _ ->

                  true = ets:delete(Monitors, Pid),
                  poolboy_manager:stop_worker(Pid),
                  {TotalAcc, WorkersAcc}

              end
          end
      end

    end, {Total, []}, Workers),


  erlang:send_after(Timeout, self(), worker_timeout),

  {noreply, State#state{total_size = NewTotal, workers = NewWorkers}};


% client or worker process DOWN
handle_info({'DOWN', Ref, _, Pid, __State}, State) ->

  true = erlang:demonitor(Ref),

  case ets:lookup(State#state.monitors, Pid) of

  % client DOWN
    [{Pid, undefined, Ref, client}] ->

      true = ets:delete(State#state.monitors, Pid),
      Waiting2 =
        queue:filter(fun({{FromPid, _}, _, _}) ->
          FromPid =/= Pid
        end, State#state.waiting),
      {noreply, State#state{waiting = Waiting2}};

  % client DOWN
    [{Pid, WorkerPid, Ref, client}] ->

      true = ets:delete(State#state.monitors, Pid),

      case process_info(WorkerPid, status) of
        undefined ->
          {noreply, State};

        _ ->

          Workers = State#state.workers,
          Waiting = State#state.waiting,
          Monitors = State#state.monitors,
          {Waiting2, Workers2} = poolboy_manager:checkin(WorkerPid, Waiting, Workers, Monitors),
          {noreply, State#state{workers = Workers2, waiting = Waiting2}}

      end;

  % worker DOWN
    [{WorkerPid, _}] ->

      true = ets:delete(State#state.monitors, WorkerPid),

      Total = State#state.total_size - 1,
      Workers = State#state.workers,
      NewWorkers = lists:keydelete(WorkerPid, 1, Workers),

      poolboy_manager:add_worker(State#state.worker_mod, State#state.worker_args),

      #state{wait_loading = WaitLoading} = State,

      NewState = State#state{
        total_size = Total,
        workers = NewWorkers,
        wait_loading = WaitLoading + 1
      },

      {noreply, NewState};

    [] ->
      {noreply, State}

  end;


handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.



to_waiting({Pid, _} = From, Timeout, Monitors, Waiting) ->
  Waiting2 = %% clean old ref and remove from queue
    case ets:lookup(Monitors, Pid) of
      [{Pid, undefined, OldRef, client}] ->
        erlang:demonitor(OldRef),
        queue:filter(fun({{FromPid, _}, _, _}) ->
          FromPid =/= Pid
        end, Waiting);

      [] ->
        Waiting
    end,

  Ref = erlang:monitor(process, Pid),
  true = ets:insert(Monitors, {Pid, undefined, Ref, client}),
  queue:in({From, Timeout, os:timestamp()}, Waiting2).