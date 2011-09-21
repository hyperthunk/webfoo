-module(xml_writer).
-compile(export_all).

-record(ctx, {
    ns          = []        :: list(tuple(string(), string())),
    stack       = []        :: [tuple(boolean(), tuple(string(), string()))],
    quote       = <<"\"">>  :: binary(),
    prettyprint = false     :: boolean(),
    indent      = <<"\t">>  :: binary(),
    newline     = <<"\n">>  :: binary(),
    %escapes = [
    %    {<<"\"">>,
    %     {binary:compile_pattern(<<"\"">>),
    %     <<"\\\"">>}}
    %]
    write
}).

-define(OPEN_BRACE, <<"<">>).
-define(FWD_SLASH, <<"/">>).
-define(CLOSE_BRACE, <<">">>).
-define(SPACE, <<" ">>).

-define(OPEN_ELEM(N), [?OPEN_BRACE, N, ?CLOSE_BRACE]).
-define(CLOSE_ELEM(N), [?OPEN_BRACE, ?FWD_SLASH, N, ?CLOSE_BRACE]).

new(WriteFun) ->
    #ctx{ write=WriteFun }.

new(RootElement, WriteFun) ->
    start(RootElement, new(WriteFun)).

set_option(prettyprint, OnOff, Writer) ->
    Writer#ctx{ prettyprint=OnOff }.

start(ElementName, Writer) ->
    start_element(ElementName, Writer).

close(Writer=#ctx{ stack=[] }) ->
    Writer;
close(Writer=#ctx{ stack=[_|_]}) ->
    close(end_element(Writer)).

with_element(Name, Writer, Fun) ->
    with_element(none, Name, Writer, Fun).

with_element(NS, Name, Writer, Fun) ->
    Writer2 = start_element(NS, Name, Writer),
    Writer3 = Fun(Writer2),
    close(Writer3).

write_attributes(AttributeList, Writer) ->
    lists:foldl(fun({Name, Value}, XMLWriter) -> 
        write_attribute(Name, Value, XMLWriter) 
    end, Writer, AttributeList).

write_attribute(Name, Value, Writer) ->
    write_attribute(none, Name, Value, Writer).

write_attribute(NS, Name, Value, Writer=#ctx{ quote=Quot }) ->
    write([?SPACE, qname(NS, Name), <<"=">>, Quot, Value, Quot], Writer).

write_value(_, #ctx{ stack=[] }) ->
    throw({error, no_root_element});
write_value(Value, Writer) ->
    write(Value, close_current_node(Writer)).

write_child(Name, Writer) ->
    write_child(none, Name, Writer).

write_child(NS, Name, Writer) ->
    start_element(NS, Name, Writer).

write_sibling(Name, Writer) ->
    write_sibling(none, Name, Writer).

write_sibling(NS, Name, Writer) ->
    Writer2 = end_element(Writer),
    start_element(NS, Name, Writer2).

start_element(Name, Writer) ->
    start_element(none, Name, Writer).

start_element(NS, Name, Writer) ->
    {Writer2, NodeName} = 
        push({NS, Name}, close_current_node(Writer)),
    write(NodeName, fun start_elem/2, Writer2).

end_element(Writer) ->
    {Writer2, NodeName} = pop(Writer),
    write(NodeName, fun end_elem/2, Writer2).

push(Scope={NS, N}, W=#ctx{ stack=Stack }) ->
    W2 = W#ctx{ stack=[{false, Scope}|Stack] },
    %% io:format("Pushing ~p~n", [N]),
    {W2, qname(NS, N)}.

pop(#ctx{ stack=[] }) ->
    throw({error, empty_stack});
pop(W=#ctx{ stack=[{false, _}|_] }) ->
    pop(close_current_node(W));
pop(W=#ctx{ stack=[{true, {NS, N}}|T] }) ->
    {W#ctx{ stack=T }, qname(NS, N)}.

close_current_node(W=#ctx{ stack=[] }) ->
    W;
close_current_node(W=#ctx{ stack=[{true, _}|_] }) ->
    W;
close_current_node(W=#ctx{ write=WF, stack=[{false, Node}|Stack] }) ->
    WF(?CLOSE_BRACE),
    W#ctx{ stack=[{true, Node}|Stack] }.

qname(none, Name) ->
    Name;
qname(NS, Name) ->
    [NS, <<":">>, Name].

write(Data, Writer=#ctx{ write=WF }) ->
    WF(Data), Writer.

write(Data, Producer, Writer=#ctx{ write=WF }) ->
    WF(Producer(Data, Writer)), Writer.

start_elem(NodeName, #ctx{ prettyprint=false }) -> 
    [?OPEN_BRACE, NodeName];
start_elem(NodeName, #ctx{ stack=S, prettyprint=true, indent=I, newline=NL }) -> 
    [NL, lists:duplicate(length(S), I), ?OPEN_BRACE, NodeName].

end_elem(NodeName, _) -> 
    [?OPEN_BRACE, ?FWD_SLASH, NodeName, ?CLOSE_BRACE].
