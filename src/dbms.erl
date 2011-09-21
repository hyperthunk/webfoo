-module(dbms).

-compile(export_all).

-record(assoc, {
    depth = 0 :: integer(),
    writer    :: term()
}).

assoc_tree(W) ->
    Writer = xml_writer:start_element("tree", W),
    #assoc{ writer=Done } = dataset:get(sql:assoc(), 
            fun build_tree/2, #assoc{ writer=Writer }),
    xml_writer:close(Done).

build_tree({_, Obj={Type, Key, Name, Vsn, _, _, Lvl, _, Ctx, Node}},
             #assoc{ depth=Depth, writer=Writer }) ->
    case Lvl of
        X when X == Depth ->
            #assoc{ depth=X, writer=write_object(Obj, Writer, fun xml_writer:write_sibling/2) };
        Y when Y > Depth orelse Y == 1 ->
            #assoc{ depth=Y, writer=write_object(Obj, Writer, fun xml_writer:write_child/2) };
        Lt when Lt < Depth ->
            Ready = lists:foldl(
                fun(_, Acc) ->
                    xml_writer:end_element(Acc) 
                end, Writer, lists:seq(0, Depth - Lt)),
            #assoc{ depth=Lt, writer=write_object(Obj, Ready, fun xml_writer:write_child/2) }
    end.

write_object({Type, Key, Name, Vsn, _, _, Lvl, _, Ctx, _}, Writer, WriteFun) ->
    Root = WriteFun(Type, Writer),
    xml_writer:write_attributes([
        {"key", Key}, 
        {"name", Name},
        {"version", Vsn},
        {"association-name", Ctx}
    ], Root).
