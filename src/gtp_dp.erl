%% Copyright 2015, Travelping GmbH <info@travelping.com>

%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version
%% 2 of the License, or (at your option) any later version.

-module(gtp_dp).

-behavior(gen_server).

%% API
-export([start_link/1, send/4,
	 create_pdp_context/2,
	 update_pdp_context/2,
	 delete_pdp_context/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("gen_socket/include/gen_socket.hrl").
-include_lib("gtplib/include/gtp_packet.hrl").
-include("include/ergw.hrl").

-record(state, {state, tref, timeout, name, node, remote_name, ip, pid, gtp_port}).

%%====================================================================
%% API
%%====================================================================

start_link({Name, SocketOpts}) ->
    gen_server:start_link(?MODULE, [Name, SocketOpts], []).

send(Socket, IP, Port, Data) ->
    gen_server:cast({global, Socket}, {send, IP, Port, Data}).

create_pdp_context(#context{data_port = GtpPort} = Context, Args) ->
    dp_call(GtpPort, create_pdp_context, Context, Args).

update_pdp_context(#context{data_port = GtpPort} = Context, Args) ->
    dp_call(GtpPort, update_pdp_context, Context, Args).

delete_pdp_context(#context{data_port = GtpPort} = Context, Args) ->
    dp_call(GtpPort, delete_pdp_context, Context, Args).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name, SocketOpts]) ->
    RemoteName = proplists:get_value(name, SocketOpts),

    State0 = #state{state = disconnected,
		    tref = undefined,
		    timeout = 10,
		    name = Name,
		    remote_name = RemoteName},
    State = connect(State0),
    {ok, State}.

handle_call(Request, _From, State) ->
    lager:error("handle_call: unknown ~p", [lager:pr(Request, ?MODULE)]),
    {reply, ok, State}.

handle_cast({send, _IP, _Port, _Data} = Msg, #state{pid = Pid} = State) ->
    lager:debug("DP Cast ~p: ~p", [Pid, Msg]),
    gen_server:cast(Pid, Msg),
    {noreply, State};

handle_cast(Msg, State) ->
    lager:error("handle_cast: unknown ~p", [lager:pr(Msg, ?MODULE)]),
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, #state{pid = Pid} = State0) ->
    State1 = handle_process_down(State0),
    State = start_process_down_timeout(State1),
    {noreply, State};

handle_info(reconnect, State0) ->
    lager:warning("trying to reconnect"),
    State = connect(State0#state{tref = undefined}),
    {noreply, State};

handle_info({packet_in, IP, Port, Msg} = Info, #state{gtp_port = GtpPort} = State) ->
    lager:debug("handle_info: ~p, ~p", [lager:pr(Info, ?MODULE), lager:pr(State, ?MODULE)]),
    gtp_context:handle_packet_in(GtpPort, IP, Port, Msg),
    {noreply, State};

handle_info(Info, State) ->
    lager:error("handle_info: unknown ~p, ~p", [lager:pr(Info, ?MODULE), lager:pr(State, ?MODULE)]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_process_down_timeout(State = #state{tref = undefined, timeout = Timeout}) ->
    NewTimeout = if Timeout < 3000 -> Timeout * 2;
		    true           -> Timeout
		 end,
    TRef = erlang:send_after(Timeout, self(), reconnect),
    State#state{tref = TRef, timeout = NewTimeout};

start_process_down_timeout(State) ->
    State.

connect(#state{name = Name, remote_name = RemoteName} = State) ->
    case global:whereis_name(RemoteName) of
        Pid when is_pid(Pid) ->
	    lager:warning("global process ~p is up", [RemoteName]),
            erlang:monitor(process, Pid),

	    {ok, _, IP} = bind(Pid),
	    ok = clear(Pid),
	    {ok, RCnt} = gtp_config:get_restart_counter(),
	    GtpPort = #gtp_port{name = Name, type = 'gtp-u', pid = self(),
				global_name = RemoteName,
				ip = IP, restart_counter = RCnt},
	    gtp_socket_reg:register(Name, GtpPort),

	    State#state{state = connected, timeout = 10, ip = IP, pid = Pid, gtp_port = GtpPort};
	_ ->
            lager:warning("global process ~p is down", [RemoteName]),
            start_process_down_timeout(State)
    end.

handle_process_down(#state{name = Name} = State) ->
    gtp_socket_reg:unregister(Name),
    State#state{state = disconnected, pid = undefined}.

%%%===================================================================
%%% Data Path Remote API
%%%===================================================================

clear(Pid) ->
    gen_server:call(Pid, clear).

bind(Pid) ->
    gen_server:call(Pid, bind).

dp_call(#gtp_port{global_name = Name}, Command,
	#context{remote_data_ip = PeerIP,
		 local_data_tei = LocalTEI, remote_data_tei = RemoteTEI}, Args) ->
    try
	gen_server:call({global, Name}, {Command, PeerIP, LocalTEI, RemoteTEI, Args})
    catch
	exit:{noproc, _} ->
	    lager:error("noproc: ~p", [Name]),
	    {error, not_found};
	exit:Exit ->
	    lager:error("Exit: ~p", [Exit]),
	    {error, not_found}
    end.
