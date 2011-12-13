-module(osmos_test).
-compile(export_all).

-include_lib("osmos/src/osmos.hrl").

test() ->
    dataset:start(),
    create_tables(),
    load_assets(),
    load_assocs(),
    select_many().

check() ->
    create_tables(),
    select_many().

create_tables() ->
    application:start(osmos),
    filelib:ensure_dir(filename:join("db", "foo")),
    create_table(asset),
    create_table(assoc, #osmos_table_format{
        block_size = 1024,
        key_format = osmos_format:binary(),
        key_less = (fun(A, B) -> A < B end),    % (KeyA, KeyB) -> bool()
        value_format = osmos_format:term(),     % #osmos_format{}
        merge = fun merge_assoc/3,              % (Key, EarlierValue, LaterValue) -> Value
        short_circuit = fun short_circuit/2,    % (Key, Value) -> bool()
        delete = fun delete_never/2             % (Key, Value) -> bool()
    }).

select_many() ->
    case osmos:select_range(asset,
            fun(K) -> K >= 999999999 end,
            fun(K) -> K =< 999999999 end,
            fun select/2, 500000) of
        {ok, Records, Continuation} ->
            io:format("Selected ~p System records... ~n", [length(Records)]);
        {error, Reason} ->
            io:format("ERROR: ~p~n", [Reason])
    end.

select_assocs() ->
    {ok, L1} = osmos:read(assoc, <<"platform-service">>),
    {ok, L2} = osmos:read(assoc, <<"service-interface">>),
    {ok, L3} = osmos:read(assoc, <<"interface-operations">>),
    EntryPoint = 28889753,  %% BPP
    L1Nodes = [ N || {EP, _}=N <- L1, EP == EntryPoint ],
    L2Nodes = [ N || {PN, _}=N <- L2, {_, L1CN} <- L1Nodes, PN == L1CN ],
    L3Nodes = [ N || {PN, _}=N <- L3, {_, L1CN} <- L2Nodes, PN == L1CN ],
    L1Nodes ++ L2Nodes ++ L3Nodes.

select(_Key, Val) -> lists:keymember(2888793, 2, Val).

short_circuit(_, _) -> true.
delete_never(_, _) -> false.
merge_assoc(_Key, Old, New) when is_list(New) ->
    lists:keymerge(1, Old, New);
merge_assoc(_Key, Old, {_, _}=New) when is_tuple(Old) ->
    [New, Old];
merge_assoc(_Key, Old, {_, _}=New) ->
    [New|Old].

create_table(T) ->
    create_table(T, osmos_table_format:new(term, term_replace, 1024)).

create_table(T, Format) ->
    Dir = filename:join("db", atom_to_list(T)),
    case osmos:open(T, [{directory, Dir},
                        {format, Format}]) of
        {ok, T} ->
            io:format("Ok - created.~n");
        {error, Reason} ->
            io:format("Error: ~p~n", [Reason])
    end.

load_assocs() ->
    Query = <<"select association_name, provider_key::integer, consumer_key::integer
               from btber.assoc">>,
    Results = dataset:get(Query, fun load_assocs/2, []),
    io:format("Inserted ~p records into assoc...~n", [length(Results)]).

load_assocs({_, Obj={Type, Pk, Ck}}, Acc) ->
    osmos:write(assoc, Type, {Pk, Ck}), [Obj|Acc].

load_assets() ->
    Query = <<"select key::integer, type_key::integer, name "
              "from btber.typed_assets">>,
    write_to_table(asset, Query).

write_to_table(Table, Query) ->
    Results = dataset:get(Query),
    Writes = [ osmos:write(Table, Key, Data) || [{key, Key}|Data] <- Results ],
    io:format("Inserted ~p records into ~p...~n", [length(Writes), Table]).
