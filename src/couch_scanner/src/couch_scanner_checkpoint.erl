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

-module(couch_scanner_checkpoint).

-export([
    write/1,
    read/0,
    reset/0
]).

-include_lib("couch/include/couch_db.hrl").

-define(DOC_ID, <<?LOCAL_DOC_PREFIX, "scanner-checkpoint">>).

% Public API

write(#{} = State) ->
    case enabled() of
        true -> with_db(fun(Db) -> update_doc(Db, ?DOC_ID, State) end);
        false -> ok
    end.

read() ->
    case enabled() of
        true -> with_db(fun(Db) -> load_doc(Db, ?DOC_ID) end);
        false -> ok
    end.

reset() ->
    case enabled() of
        true -> with_db(fun(Db) -> delete_doc(Db, ?DOC_ID) end);
        false -> ok
    end.

% Private functions

delete_doc(Db, DocId) ->
    case couch_db:open_doc(Db, DocId, []) of
        {ok, #doc{revs = {_, RevList}}} ->
            {ok, _} = couch_db:delete_doc(Db, DocId, RevList),
            ok;
        {not_found, _} ->
            not_found
    end.

update_doc(Db, DocId, #{} = Body) ->
    EJsonBody = ?JSON_DECODE(?JSON_ENCODE(Body#{<<"_id">> => DocId})),
    Doc = couch_doc:from_json_obj(EJsonBody),
    case couch_db:open_doc(Db, DocId, []) of
        {ok, #doc{revs = Revs}} ->
            {ok, _} = couch_db:update_doc(Db, Doc#doc{revs = Revs}, []);
        {not_found, _} ->
            {ok, _} = couch_db:update_doc(Db, Doc, [])
    end,
    ok.

load_doc(Db, DocId) ->
    case couch_db:open_doc(Db, DocId, [ejson_body]) of
        {ok, #doc{body = EJsonBody}} ->
            ?JSON_DECODE(?JSON_ENCODE(EJsonBody), [return_maps]);
        {not_found, _} ->
            not_found
    end.

with_db(Fun) ->
    DbName = config:get("mem3", "shards_db", "_dbs"),
    case mem3_util:ensure_exists(DbName) of
        {ok, Db} ->
            try
                Fun(Db)
            after
                catch couch_db:close(Db)
            end;
        Else ->
            throw(Else)
    end.

enabled() ->
    config:get_boolean("couch_scanner", "persist", false).
