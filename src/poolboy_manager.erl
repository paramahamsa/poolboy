-module(poolboy_manager).

-export([checkin/4, start_workers/3, stop_worker/1, add_worker/2]).



start_workers(0, _, _)->
    ok;
start_workers(Count, WorkerMod, Args)->    
   
    Pool = self(),

    spawn(fun()->       

        lists:foreach(fun(_)-> 

            try
                {ok, Pid} = WorkerMod:start_link(Args),
                gen_server:cast(Pool, {add_worker, Pid})
            catch _:_ ->
                gen_server:cast(Pool, worker_crash_in_start)
            end

        end, lists:seq(1, Count))

    end).


stop_worker(Pid)->
    spawn(fun()-> 
        try
            gen_server:call(Pid, stop, 500)
        catch _:_ ->
            exit(Pid, kill)
        end
    end).
    



% start new worker
add_worker(WorkerMod, Args) ->
    
    Pool = self(),
    
     spawn(fun()->       

        try
            {ok, Pid} = WorkerMod:start_link(Args),
            gen_server:cast(Pool, {add_worker, Pid})
        catch _:_ ->
            gen_server:cast(Pool, worker_crash_in_start)
        end

     end).


 
checkin(WorkerPid, Waiting, Workers, Monitors)->

	case queue:out(Waiting) of
		{{value, {{FromPid, _} = From, Timeout, StartTime}}, Left} ->
		
			case wait_valid(StartTime, Timeout) of
				true ->
						
						true = ets:update_element(Monitors, FromPid, {2, WorkerPid}),
						gen_server:reply(From, WorkerPid),
						{Left, Workers};
		
				false ->
						checkin(WorkerPid, Left, Workers, Monitors)

			end;
       
		{empty, Empty} ->
			{Empty, [{WorkerPid, os:timestamp()} | Workers]}

	end.




wait_valid(_StartTime, infinity) ->
    true;
wait_valid(StartTime, Timeout) ->
    Waited = timer:now_diff(os:timestamp(), StartTime),
    (Waited div 1000) < Timeout.