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

-module(couch_scanner_test).

-include_lib("couch/include/couch_eunit.hrl").

couch_scanner_test_() ->
    {
        setup,
        fun start_couch/0,
        fun stop_couch/1,
        with([
            ?TDEF(t_scanner)
        ])
    }.

start_couch() ->
    test_util:start_config().

stop_couch(Ctx) ->
    test_util:stop_couch(Ctx).

t_scanner(_Ctx) ->
    ok.
