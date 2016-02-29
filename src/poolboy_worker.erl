-module(poolboy_worker).

-callback start_link( any() ) -> {ok, pid()}.

