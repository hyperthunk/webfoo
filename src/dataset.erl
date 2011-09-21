-module(dataset).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/0, stop/0, get/1, get/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start() ->
    gen_server:start({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

get(Query) ->
    gen_server:call(?SERVER, {get, Query}).

get(Query, RowCallback, InitVal) ->
    gen_server:call(?SERVER, {get, Query, RowCallback, InitVal}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_) ->
    pgsql:connect("localhost", "postgres", [{database, "postgres"}]).

handle_call({get, Query, RowCallback, InitVal}, _From, Connection) ->
    case pgsql:equery(Connection, Query) of
        {ok, Columns, Rows} ->
            DataSet = lists:foldl(fun(Row, Acc) -> RowCallback({Columns, Row}, Acc) end, InitVal, Rows),
            {reply, DataSet, Connection};
        Other ->
            io:format("Bad Result: ~p~n", [Other]),
            {reply, error, Connection}
    end;
handle_call({get, Query}, _From, Connection) ->
    case pgsql:equery(Connection, Query) of
        {ok, Columns, Rows} ->
            Cols = [ binary_to_atom(element(2, T), utf8) || T <- Columns ],
            DataSet = lists:map(fun(Row) -> lists:zip(Cols, tuple_to_list(Row)) end, Rows),
            {reply, DataSet, Connection};
        Other ->
            io:format("Bad Result: ~p~n", [Other]),
            {reply, error, Connection}
    end;
handle_call(stop, _, Connection) ->
    {stop, normal, Connection}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, Conn) ->
    pgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

