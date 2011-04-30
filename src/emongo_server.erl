-module(emongo_server).

-behaviour(gen_server).

-include("emongo.hrl").

-export([start_link/2]).

-export([send/3, send/2, send_recv/4]).
-export([send_recv_nowait/3, recv/4]).

-deprecated([send/3]).

%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool_id, socket, requests, leftover}).

%% messages
-define(abort(ReqId), {abort, ReqId}).
-define(send(Packet), {send, Packet}).
-define(send_recv(ReqId, Packet, From),
        {send_recv, ReqID, Packet, From}).

%% to be removed next release
-define(old_send(ReqId, Packet), {send, ReqId, Packet}).

start_link(PoolId, Urls) ->
    gen_server:start_link(?MODULE, [PoolId, Urls], []).


send(Pid, _ReqID, Packet) ->
    send(Pid, Packet).

send(Pid, Packet) ->
    gen_server:cast(Pid, ?send(Packet)).


send_recv_nowait(Pid, ReqID, Packet) ->
    Tag = make_ref(),
    gen_server:cast(Pid, ?send_recv(ReqID, Packet, {self(), Tag})),
    Tag.


recv(Pid, ReqID, 0, Tag) ->
    Pid ! ?abort(ReqID),
    receive
        {Tag, Resp} ->
            Documents = emongo_bson:decode(Resp#response.documents),
            Resp#response{documents=Documents}
    after 0 ->
            exit(emongo_timeout)
    end;

recv(Pid, ReqID, Timeout, Tag) ->
    receive
        {Tag, Resp} ->
            Documents = emongo_bson:decode(Resp#response.documents),
            Resp#response{documents=Documents}
    after Timeout ->
            recv(Pid, ReqID, 0, Tag)
    end.


send_recv(Pid, ReqID, Packet, Timeout) ->
    Tag = send_recv_nowait(Pid, ReqID, Packet),
    recv(Pid, ReqID, Timeout, Tag).


%% gen_server %%

init([PoolId, Urls]) ->
    case get_master(Urls) of
        {socket,Socket,Rem} ->
            inet:setopts(Socket,[{active,true}]),
            {ok, #state{pool_id=PoolId, socket=Socket, requests=[], leftover = Rem}};
        {error, Reason} ->
            {stop, {failed_to_open_socket, Reason}}
    end.


handle_call(_Request, _From, State) ->
    {reply, undefined, State}.


handle_cast(?send_recv(ReqID, Packet, From), State) ->
    case is_aborted(ReqID) of
        true ->
            {noreply, State};
        _ ->
            gen_tcp:send(State#state.socket, Packet),
            State1 = State#state{requests=[{ReqID, From} | State#state.requests]},
            {noreply, State1}
    end;

handle_cast(?old_send(_ReqId, Packet), State) ->
    gen_tcp:send(State#state.socket, Packet),
    {noreply, State};

handle_cast(?send(Packet), State) ->
    gen_tcp:send(State#state.socket, Packet),
    {noreply, State}.


handle_info(?abort(ReqId), #state{requests=Requests}=State) ->
    State1 = State#state{requests=lists:keydelete(ReqId, 1, Requests)},
    {noreply, State1};

handle_info({tcp, _Socket, Data}, State) ->
    Leftover = <<(State#state.leftover)/binary, Data/binary>>,
    {noreply, process_bin(State#state{leftover= <<>>}, Leftover)};

handle_info({tcp_closed, _Socket}, _State) ->
    exit(tcp_closed);

handle_info({tcp_error, _Socket, Reason}, _State) ->
    exit({tcp_error, Reason}).


terminate(_, State) -> gen_tcp:close(State#state.socket).


code_change(_Old, State, _Extra) -> {ok, State}.

%% internal


process_bin(State, <<>>) ->
    State;

process_bin(State, Bin) ->
    case emongo_packet:decode_response(Bin) of
        undefined ->
            State#state{leftover=Bin};

        {Resp, Tail} ->
            ResponseTo = (Resp#response.header)#header.response_to,
            
            case lists:keytake(ResponseTo, 1, State#state.requests) of
                false ->
                    cleanup_cursor(Resp, ResponseTo, State),
                    process_bin(State, Tail);

                {value, {_, From}, Requests} ->
                    case is_aborted(ResponseTo) of
                        false ->
                            gen_server:reply(
                              From,
                              Resp#response{pool_id=State#state.pool_id});
                        true ->
                            cleanup_cursor(Resp, ResponseTo, State)
                    end,
                    process_bin(State#state{requests=Requests}, Tail)
            end
    end.


is_aborted(ReqId) ->
    receive
        ?abort(ReqId) ->
            true
    after 0 ->
            false
    end.

cleanup_cursor(#response{cursor_id=0}, _, _) ->
    ok;
cleanup_cursor(#response{cursor_id=CursorID}, ReqId, State) ->
    Packet = emongo_packet:kill_cursors(ReqId, [CursorID]),
    gen_tcp:send(State#state.socket, Packet).

get_master(Urls) -> get_master(Urls,noerror).

get_master([],LastError) -> LastError;
get_master([Url|Urls],_) ->
    case get_ismaster(Url) of
        {ok,Sock,Doc,Rem} -> 
            IsMaster = proplists:get_value(<<"ismaster">>,Doc),
            Primary = proplists:get_value(<<"primary">>,Doc),
            case {IsMaster,Primary} of
		{true,_} -> {socket,Sock,Rem};
                {false,undefined} -> gen_tcp:close(Sock), get_master(Urls,{error,no_primary});
                {false,OtherHost} -> gen_tcp:close(Sock), get_master([OtherHost])
            end;
	Other -> get_master(Urls,Other)
    end.

get_ismaster({H,P}) when is_list(P)     -> get_ismaster({H,list_to_integer(P)});
get_ismaster({H,P}) when is_atom(H)     -> get_ismaster({atom_to_list(H),P});
get_ismaster(Url)   when is_binary(Url) -> get_ismaster(binary_to_list(Url));
get_ismaster(Url)   when is_list(Url)   ->
    case string:tokens(Url,":") of
        [H,P] when is_list(H) -> get_ismaster({H,list_to_integer(P)});
        [H] -> get_ismaster({H,27017})
    end;

get_ismaster({Host,Port}) when is_list(Host), is_integer(Port) ->
    case gen_tcp:connect(Host, Port, [binary, {active, false}, {nodelay, true}], ?TIMEOUT) of
        {ok,Sock} -> 
            Query = emongo_packet:do_query("admin","$cmd",1,#emo_query{q=[{"ismaster",1}],limit=1}),
            gen_tcp:send(Sock,Query),
            {ok,{Resp,Rem}} = sync_get_frame(Sock,<<>>),
            1 = (Resp#response.header)#header.response_to, %%Ensure it's a response to this message
            [IsMaster] = emongo_bson:decode(Resp#response.documents),
	    {ok,Sock,IsMaster,Rem};
        Err -> Err
    end.

sync_get_frame(Sock,Rem) ->
    case gen_tcp:recv(Sock,0,1000) of
        {ok,Bin} -> 
            FullBin = <<Rem/binary,Bin/binary>>,
            case emongo_packet:decode_response(FullBin) of
                undefined -> sync_get_frame(Sock,FullBin);
                R = {_,_} -> {ok,R}
            end;
        Other -> Other
    end.


