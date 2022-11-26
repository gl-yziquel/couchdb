// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

#include "quickjs.h"
#include "quickjs-libc.h"

#define DEFAULT_STACK_SIZE (64L * 1024L * 1024L)
#define BUF_SIZE 1024

#define USAGE "couchjs [-V|-M memorylimit|-h] <script.js>\n"

#define BAIL(error) {fprintf(stderr, "%s:%d %s\n", __FILE__, __LINE__, error);\
  exit(EXIT_FAILURE);\
}
#define BAILJS(cx, error) {fprintf(stderr, "%s:%d %s\n", __FILE__, __LINE__, error);\
  js_std_dump_error(cx);\
  exit(EXIT_FAILURE);\
}

// These are auto-generated by qjsc
extern const uint32_t bytecode_size;
extern const uint8_t bytecode[];

typedef struct {
    int             stack_size;
} couch_args;

typedef enum {CMD_EMPTY, CMD_DDOC, CMD_RESET, CMD_LIST, CMD_VIEW} CmdType;

static void parse_args(int argc, const char* argv[], couch_args* args)
{
    int i = 1;
    while(i < argc) {
        if (strncmp("-h", argv[i], 2) == 0) {
            fprintf(stderr, USAGE);
            exit(0);
        } else if (strncmp("-M", argv[i], 2) == 0) {
            args->stack_size = atoi(argv[++i]);
            if (args->stack_size <= 1L * 1024L * 1024L) {
              BAIL("Invalid stack size");
            }
        } else if (strncmp("-V", argv[i], 2) == 0) {
            fprintf(stderr, "quickjs\n");
            exit(0);
        } else {
            break;
        }
        i++;
    }
}

// Parse the command type. We only care about resets, ddoc operations and
// making sure wires were not crossed and we ended up in a list streaming
// sub-command state somehow. See protocol description at:
//   https://docs.couchdb.org/en/stable/query-server/protocol.html
//
static CmdType parse_command(char* str, size_t len) {
  if (len == 0) {
    return CMD_EMPTY;
  }
  if (len >= 8  && strncmp("[\"reset\"", str, 8) == 0) {
    return CMD_RESET;
  }
  if (len >= 7  && strncmp("[\"ddoc\"", str, 7) == 0) {
    return CMD_DDOC;
  }
  if (len >= 11 && strncmp("[\"list_row\"", str, 11) == 0) {
    return CMD_LIST;
  }
  if (len >= 11 && strncmp("[\"list_end\"", str, 11) == 0) {
    return CMD_LIST;
  }
  return CMD_VIEW;
}

static void add_cx_methods(JSContext* cx) {
  //TODO: configure some features with env vars of command line switches
  JS_AddIntrinsicBaseObjects(cx);
  JS_AddIntrinsicEval(cx);
  JS_AddIntrinsicJSON(cx);
  JS_AddIntrinsicRegExp(cx);
  JS_AddIntrinsicMapSet(cx);
  JS_AddIntrinsicDate(cx);
}

// Creates a new JSContext with only the provided sandbox function
// in its global. Make sure to free the context when done with it.
//
static JSContext* make_sandbox(JSContext* cx, JSValue sbox) {
   JSContext *cx1 = JS_NewContextRaw(JS_GetRuntime(cx));
   if(!cx1) {
     return NULL;
   }
   add_cx_methods(cx1);
   JSValue global = JS_GetGlobalObject(cx1);

   int i;
   JSPropertyEnum *tab;
   uint32_t tablen;
   JSValue prop_val;

   int prop_flags = JS_GPN_STRING_MASK | JS_GPN_ENUM_ONLY;
   if(JS_GetOwnPropertyNames(cx, &tab, &tablen, sbox, prop_flags) < 0){
     JS_FreeContext(cx1);
     return NULL;
   }
   for(i=0; i < tablen; i++) {
     prop_val = JS_GetProperty(cx, sbox, tab[i].atom);
     if (JS_IsException(prop_val)) {
       goto exception;
     }
     JS_SetProperty(cx1, global, tab[i].atom, prop_val);
   }

   for(i=0; i < tablen; i++) {
     JS_FreeAtom(cx, tab[i].atom);
   }

   js_free(cx, tab);
   JS_FreeValue(cx1, global);
   return cx1;

exception:
  for(i = 0; i < tablen; i++) {
    JS_FreeAtom(cx, tab[i].atom);
  }
  js_free(cx, tab);
  JS_FreeValue(cx1, global);
  JS_FreeContext(cx1);
  return NULL;
}

// This is mostly for test compatibility between engines, and
// some anti-footgun help for user code. For real sandboxing we rely on
// destroying and re-creating the whole JSRuntime instance.
//
static JSValue js_evalcx(JSContext* cx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    size_t strlen;
    const char *str;
    const char *name;

    if (argc != 3) {
      return JS_EXCEPTION;
    }

    if(!JS_IsObject(argv[1])) {
      return JS_EXCEPTION;
    }
    JSValue sbox = argv[1];

    str = JS_ToCStringLen(cx, &strlen, argv[0]);
    if(!str) {
      return JS_EXCEPTION;
    }

    name = JS_ToCString(cx, argv[2]);
    if(!name) {
      JS_FreeCString(cx, str);
      return JS_EXCEPTION;
    }

    JSContext *cx1 = make_sandbox(cx, sbox);
    if(!cx1) {
      JS_FreeCString(cx, str);
      JS_FreeCString(cx, name);
      return JS_EXCEPTION;
    }

    int flags = JS_EVAL_TYPE_GLOBAL | JS_EVAL_FLAG_BACKTRACE_BARRIER;
    JSValue res = JS_Eval(cx1, str, strlen, name, flags);

    JS_FreeCString(cx, str);
    JS_FreeCString(cx, name);
    JS_FreeContext(cx1);
    return res;
}

// Once list/show features are gone, could avoid this too and just have the
// dispatch return the list of rows as a response. That way JS can entirely
// avoid any IO logic, only take rows and return rows in a simple
// request/response manner.
//
static JSValue js_print(JSContext* cx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    const char *str;
    if (argc == 1) {
      str = JS_ToCString(cx, argv[0]);
      if (!str) {
        return JS_UNDEFINED;
      }
      fputs(str, stdout);
      JS_FreeCString(cx, str);
    } else if (argc > 1) {
      return JS_EXCEPTION;
    }
    fputc('\n', stdout);
    fflush(stdout);
    return JS_UNDEFINED;
}

//TODO: remove when lists/show are gone. The only reason to have this function
//is to support getRow() for lists.
//
static JSValue js_readline(JSContext* cx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    if (argc != 0) return JS_EXCEPTION;

    JSValue res;
    size_t linemax = BUF_SIZE;
    char* line = malloc(linemax);
    if(!line) {
      BAIL("Could not allocate line buffer for list sub-command");
    }
    int len;
    if ((len = getline(&line, &linemax, stdin)) != -1) {
      if (line[len - 1] != '\n') {
        BAIL("list getline() didn't end in newline");
      }
      line[--len] = '\0'; // don't care about the last \n so shorten the string
      switch (parse_command(line, len)) {
        case CMD_LIST:
          res = JS_NewStringLen(cx, (const char*)line, len);
          break;
        case CMD_EMPTY:
          res = JS_NewString(cx, "");
          break;
        default:
          BAIL("unexpected command during list subcommand mode");
      }
      free(line);
      return res;
    } else {
      free(line);
      return JS_EXCEPTION;
    }
}

// TODO: This may not be neeed. Mainly for SM API compat to minimize main.js differences
//
static JSValue js_gc(JSContext* cx, JSValueConst this_val, int argc, JSValueConst *argv)
{
    if (argc != 0) {
      return JS_EXCEPTION;
    }
    JS_RunGC(JS_GetRuntime(cx));
    return JS_UNDEFINED;
}

static void free_cx(JSContext* cx) {
  if (cx == NULL) {
    return;
  }
  JSRuntime* rt = JS_GetRuntime(cx);
  if (rt == NULL) {
    BAIL("JSRuntime is unexpectedly NULL");
  }
  JS_FreeContext(cx);
  JS_FreeRuntime(rt);
}

static JSContext* new_cx(const couch_args* args) {
  JSRuntime* rt;
  JSContext* cx;

  rt = JS_NewRuntime();
  if (rt == NULL) {
    BAIL("Could not create JSRuntime");
  }

  JS_SetMemoryLimit(rt, args->stack_size);
  JS_SetMaxStackSize(rt, args->stack_size);

  cx = JS_NewContextRaw(rt);
  if (cx == NULL) {
    BAIL("Could not create JSContext");
  }

  add_cx_methods(cx);
  return cx;
}

// This is what we rely on for sandboxing. On a reset command, blow away
// the whole runtime instance and re-create it by re-evaluating the bytecode again
// in a new instance.
//
static JSContext* reset_cx(const couch_args* args, JSContext *cx) {
  JSValue global, obj, val;

  free_cx(cx);
  cx = new_cx(args);

  global = JS_GetGlobalObject(cx);
  JS_SetPropertyStr(cx, global, "print",    JS_NewCFunction(cx, js_print,   "print",    1));
  JS_SetPropertyStr(cx, global, "readline", JS_NewCFunction(cx, js_readline,"readline", 0));
  JS_SetPropertyStr(cx, global, "gc",       JS_NewCFunction(cx, js_gc,      "gc",       0));
  JS_SetPropertyStr(cx, global, "evalcx",   JS_NewCFunction(cx, js_evalcx,  "evalcx",   3));

  obj = JS_ReadObject(cx, bytecode, bytecode_size,  JS_READ_OBJ_BYTECODE);
  if (JS_IsException(obj)) {
    BAILJS(cx, "Error reading bytecode");
  }
  val = JS_EvalFunction(cx, obj); // this calls auto-frees obj
  if (JS_IsException(val)) {
    BAILJS(cx, "Error evaluating bytecode");
  }
  JS_FreeValue(cx, val);
  JS_FreeValue(cx, global);
  return cx;
}

// Dispatch a single command line to the engine. If it weren't for list functions we could have
// made it return the responses as a result too.
//
// The result is a boolean value indicating whether to continue processing or stop and exit.
//
static bool dispatch(JSContext* cx, char* str, size_t len) {
  JSValue global = JS_GetGlobalObject(cx);

  JSValue fun = JS_GetPropertyStr(cx, global, "dispatch");
  if (JS_IsException(fun)) {
    BAILJS(cx, "Could not find main dispatch function");
  }
  if (!JS_IsFunction(cx, fun)) {
    BAIL("dispatch is not a function");
  }

  JSValue argv[] = {JS_NewStringLen(cx, str, len)};
  JSValue jres = JS_Call(cx, fun, global, 1, argv);
  if (JS_IsException(jres)) {
    BAILJS(cx, "couchjs internal error");
  }
  if (!JS_IsBool(jres)) {
    BAIL("dispatch didn't return boolean value");
  }
  bool res = JS_VALUE_GET_BOOL(jres);

  JS_FreeValue(cx, jres);
  JS_FreeValue(cx, argv[0]);
  JS_FreeValue(cx, fun);
  JS_FreeValue(cx, global);

  return res;
}

int main(int argc, const char* argv[])
{
    JSContext* view_cx = NULL;
    JSContext* ddoc_cx = NULL;

    couch_args args = {.stack_size = DEFAULT_STACK_SIZE};
    parse_args(argc, argv, &args);
    //load_bytecode(&args);

    size_t linemax = BUF_SIZE;
    char* line = malloc(linemax);
    if (!line) {
      BAIL("Could not allocate line buffer");
    }

    int len;
    bool do_continue = true;
    while (do_continue && (len = getline(&line, &linemax, stdin)) != -1) {
      if (line[len - 1] != '\n') {
        BAIL("getline() didn't end in newline");
      }
      line[--len] = '\0'; // don't care about the last \n so shorten the string
      switch (parse_command(line, len)) {
          case CMD_RESET:
            view_cx = reset_cx(&args, view_cx);
            do_continue = dispatch(view_cx, line, len);
            break;
          case CMD_DDOC:
            if (ddoc_cx == NULL) {
              ddoc_cx = reset_cx(&args, NULL);
            }
            do_continue = dispatch(ddoc_cx, line, len);
            break;
          case CMD_VIEW:
            if (view_cx == NULL) {
              view_cx = reset_cx(&args, NULL);
            }
            do_continue = dispatch(view_cx, line, len);
            break;
         case CMD_EMPTY:
            do_continue = false;
            break;
         case CMD_LIST:
            BAIL("unexpected list subcommand in the main command loop");
      }
    }

    free_cx(view_cx);
    free_cx(ddoc_cx);
    free(line);

    return EXIT_SUCCESS;
}

