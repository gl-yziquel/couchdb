% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_scanner_server).

%-include_lib("couch/include/couch_db.hrl").

-export([
    start_link/0,
    enable/0,
    disable/0,
    status/0
]).

-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export([
    handle_config_change/5,
    handle_config_terminate/3
]).

-define(CHECKPOINT_INTERVAL_MSEC, 180000).
-define(CONFIG_RELISTEN_MSEC, 5000).

-record(st, {
    enabled,
    started,
    db_cursor,
    checkpoint_st,
    cluster_state
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

enable() ->
    gen_server:call(?MODULE, enable).

disable() ->
    gen_server:call(?MODULE, disable).

status() ->
    gen_server:call(?MODULE, status).

init(_Args) ->
    process_flag(trap_exit, true),
    ok = config:listen_for_changes(?MODULE, nil),
    St = #st{
        enabled = config:get_boolean("couch_scanner", "enabled", false),
        started = 0,
        db_cursor = <<>>,
        checkpoint_st = #{},
        cluster_state = {[node() | nodes()], mem3:nodes()}
    },
    {ok, St, {continue, unpersist}}.

handle_continue(unpersist, #st{} = St) ->
    UnpersistState = couch_scanner_persist:unpersist(),
    couch_log:info("~p : unpersist state ~p", [?MODULE, UnpersistState]),
    schedule_checkpoint(),
    St1 = start(St),
    {noreply, St1}.

handle_call(enable, _From, #st{enabled = true} = St) ->
    {reply, ok, St};
handle_call(enable, _From, #st{enabled = false} = St) ->
    couch_log:info("~p : enable", [?MODULE]),
    {reply, ok, start(St#st{enabled = true})};
handle_call(disable, _From, #st{enabled = false} = St) ->
    {reply, ok, St};
handle_call(disable, _From, #st{enabled = true} = St) ->
    couch_log:info("~p : disable", [?MODULE]),
    {reply, ok, stop(St#st{enabled = false})};
handle_call(status, _From, #st{} = St) ->
    {reply, St, St}.

handle_cast(Msg, #st{} = St) ->
    couch_log:error("~p : unknown cast ~p", [?MODULE, Msg]),
    {noreply, St}.

handle_info(checkpoint, #st{} = St) ->
    St1 = checkpoint(St),
    schedule_checkpoint(),
    {noreply, St1};
handle_info(restart_config, #st{} = St) ->
    k = config:listen_for_changes(?MODULE, nil),
    {noreply, St};
handle_info(Msg, St) ->
    couch_log:error("~p : unknown info message ~p", [?MODULE, Msg]),
    {noreply, St}.

handle_config_change("couch_scanner", "enabled", "true", _, S) ->
    enable(),
    {ok, S};
handle_config_change("couch_scanner", "enabled", "false", _, S) ->
    disable(),
    {ok, S};
handle_config_change(_, _, _, _, _) ->
    {ok, nil}.

handle_config_terminate(_Server, stop, _State) ->
    ok;
handle_config_terminate(_Server, _Reason, _State) ->
    erlang:send_after(?CONFIG_RELISTEN_MSEC, whereis(?MODULE), restart_config).

schedule_checkpoint() ->
    erlang:send_after(?CHECKPOINT_INTERVAL_MSEC, self(), checkpoint).

start(#st{enabled = false} = St) ->
    St;
start(#st{enabled = true} = St) ->
    % check config module
    % check if resuming from state
    % if stale reset
    % spawn db folder
    % spawn isolated runner for each module
    St#st{started = erlang:system_time(second)}.

stop(#st{enabled = false} = St) ->
    St;
stop(#st{enabled = true} = St) ->
    % maybe checkpoint
    % stop runners
    % stop db folder
    St#st{enabled = false}.

checkpoint(#st{enabled = false} = St) ->
    St;
checkpoint(#st{enabled = true} = St) ->
    #st{checkpoint_st = PrevCheckpointSt} = St,
    #st{db_cursor = DbCursor} = St,
    CheckpointSt = #{db_cursor => DbCursor},
    case PrevCheckpointSt == CheckpointSt of
        true ->
            St;
        false ->
            ok = couch_scanner_persist:persist(CheckpointSt),
            St#st{checkpoint_st = CheckpointSt}
    end.
