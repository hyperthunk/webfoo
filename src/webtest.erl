-module(webtest).

-export([start/0, stop/0]).

% start misultin http server
start() ->
    dataset:start(),
    misultin:start_link([{port, 8080}, {loop, fun(Req) -> handle_http(Req) end}]).

% stop misultin
stop() ->
    dataset:stop(),
    misultin:stop().

% callback on request received
handle_http(Req) ->
    io:format("Processing request...."),
    % send headers
    %Req:chunk(head, [{"Content-Type", "application/xml"}]),
    % send chunk
    %Req:chunk(<<"<results>">>),
    
    %lists:map(
    %fun(Asset) -> 
    %    {ok, IoList} = platforms:render(Asset),
    %    Req:chunk(IoList) 
    %end, dataset:all()),
    
    % send chunk
    %Req:chunk(<<"</results>">>),
    %Req:chunk(done).
    case Req:resource([lowercase, urldecode]) of
        ["systems"] ->
            Query = sql:systems(), 
            Template = systems,
            {ok, Res} = Template:render([{results, dataset:get(Query)}]),
            Req:ok([{"Content-Type", "application/xml"}], Res);
        ["assoc"] ->
            Req:stream(head, [{"Content-Type", "text/xml"}]),
            W = xml_writer:new(fun(D) -> Req:stream(D) end),
            dbms:assoc_tree(W),
            Req:stream(close)
    end.
