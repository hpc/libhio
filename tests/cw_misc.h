//============================================================================
// cw_misc.h - various small functions and macros to simplify the writing
// of utility programs.
//============================================================================
#ifndef CW_MISC_H_INCLUDED
#define CW_MISC_H_INCLUDED
#include <stdio.h>

// Common typedefs and macros
typedef unsigned long long int U64;
typedef long long int I64;
#define DIM1(array) ( sizeof(array) / sizeof(array[0]) )

//----------------------------------------------------------------------------
// Message control macros and functions
//----------------------------------------------------------------------------
typedef struct msg_context {
  char * id_string;
  int verbose_level;
  int debug_level;
  FILE * std_file;
  FILE * err_file;
} MSG_CONTEXT;

void msg_context_init(MSG_CONTEXT *msgctx, int verbose_level, int debug_level);
void msg_context_set_verbose(MSG_CONTEXT *msgctx, int verbose_level);
void msg_context_set_debug(MSG_CONTEXT *msgctx, int debug_level);
void msg_context_free(MSG_CONTEXT *msgctx);
void msg_writer(MSG_CONTEXT *msgctx, FILE * stream, const char *format, ...);

//----------------------------------------------------------------------------
// Preprocessor variable MY_MSG_CTX must be defined to an expression that
// resolves to the address of a msg_context structure.  It is used by the MSG,
// MSGE, ERRX, DBG<n> and IFDBG<n> macros.  It may be redefined in different
// sections of a program.   
//
// Example:
//   MSG_CONTEXT my_msg_context;
//   #define MY_MSG_CTX (&my_msg_context)
//   msg_context_init(MY_MSG_CTX, 1, 3);
//   MY_MSG_CTX->id_string = "cw_misc_test ";
//----------------------------------------------------------------------------
#define MSG(...)   msg_writer((MY_MSG_CTX), (MY_MSG_CTX)->std_file, __VA_ARGS__);
#define MSGE(...)  msg_writer((MY_MSG_CTX), (MY_MSG_CTX)->err_file, __VA_ARGS__);

#define ERRX(...) {                  \
  MSGE("Error: " __VA_ARGS__);       \
  exit(12);                          \
}

//----------------------------------------------------------------------------
// DBGMAXLEV controls which debug messages are compiled into the program.
// Set via compile option -DDBGLEV=<n>, where <n> is 0, 1, 2, etc.
// The default value is DBGMAXLEV = 4.  The purpose of this is to make
// it possible to include debug messages in a program that may impact
// performance if tested on every iteration, but only compile those debug
// messages into the program if requested at compile time by setting 
// DBGMAXLEV to a higher than normal value.
//----------------------------------------------------------------------------
#ifndef DBGMAXLEV
  #define DBGMAXLEV 4
#endif

#define DBGX(n, ...) if ((MY_MSG_CTX)->debug_level >= (n)) MSG("Debug: " __VA_ARGS__);
#define IFDBGX(n, ...) if ((MY_MSG_CTX)->debug_level >= (n)) { __VA_ARGS__; };

#define DBG0(...) MSGE("Debug: " __VA_ARGS__);
#define IFDBG0(...) { __VA_ARGS__; };

#if DBGMAXLEV >= 1
  #define DBG1(...) DBGX(1, __VA_ARGS__)
  #define IFDBG1(...) IFDBGX(1, __VA_ARGS__)
#else
  #define DBG1(...)
  #define IFDBG1(...)
#endif

#if DBGMAXLEV >= 2
  #define DBG2(...) DBGX(2, __VA_ARGS__)
  #define IFDBG2(...) IFDBGX(2, __VA_ARGS__)
#else
  #define DBG2(...)
  #define IFDBG2(...)
#endif

#if DBGMAXLEV >= 3
  #define DBG3(...) DBGX(3, __VA_ARGS__)
  #define IFDBG3(...) IFDBGX(3, __VA_ARGS__)
#else
  #define DBG3(...)
  #define IFDBG3(...)
#endif

#if DBGMAXLEV >= 4
  #define DBG4(...) DBGX(4, __VA_ARGS__)
  #define IFDBG4(...) IFDBGX(4, __VA_ARGS__)
#else
  #define DBG4(...)
  #define IFDBG4(...)
#endif

#if DBGMAXLEV >= 5
  #define DBG5(...) DBGX(5, __VA_ARGS__)
  #define IFDBG5(...) IFDBGX(5, __VA_ARGS__)
#else
  #define DBG5(...)
  #define IFDBG5(...)
#endif

#define MAX_VERBOSE 3
#define VERB0(...) MSG(__VA_ARGS__);
#define VERB1(...) if ((MY_MSG_CTX)->verbose_level >= 1) MSG(__VA_ARGS__);
#define VERB2(...) if ((MY_MSG_CTX)->verbose_level >= 2) MSG(__VA_ARGS__);
#define VERB3(...) if ((MY_MSG_CTX)->verbose_level >= 3) MSG(__VA_ARGS__);

//----------------------------------------------------------------------------
// Wrappers for functions that manage memory which will check results and
// call ERRX on failure.
//----------------------------------------------------------------------------
void *mallocx(MSG_CONTEXT *msgctx, const char *context, size_t size);
void *reallocx(MSG_CONTEXT *msgctx, const char *context, void *ptr, size_t size);
void *freex(MSG_CONTEXT *msgctx, const char *context, void *ptr);
char *strndupx(MSG_CONTEXT *msgctx, const char *context, const char *s1, size_t n);
char *strcatrx(MSG_CONTEXT *msgctx, const char *context, const char *s1, const char *s2);
char *alloc_printf(MSG_CONTEXT *msgctx, const char *context, const char *format, ...);

// Macro wrappers for the above to supply message context and source line
#define STRINGIFY(n) STRINGIFY_HELPER(n)
#define STRINGIFY_HELPER(n) #n
#define SOURCE_FILE_LINE_STRING __FILE__ "(" STRINGIFY(__LINE__) ")"
#define MALLOCX(size) mallocx((MY_MSG_CTX), SOURCE_FILE_LINE_STRING, (size))
#define REALLOCX(ptr, size) reallocx((MY_MSG_CTX), SOURCE_FILE_LINE_STRING, (ptr), (size))
#define FREEX(ptr) freex((MY_MSG_CTX), SOURCE_FILE_LINE_STRING, (ptr))
#define STRNDUPX(s1, n) strndupx((MY_MSG_CTX), SOURCE_FILE_LINE_STRING, (s1), (n))
#define STRCATRX(s1, s2) strcatrx((MY_MSG_CTX), SOURCE_FILE_LINE_STRING, (s1), (s2))
#define ALLOC_PRINTF(...) alloc_printf((MY_MSG_CTX), SOURCE_FILE_LINE_STRING, __VA_ARGS__)

//----------------------------------------------------------------------------
// Enum name/value conversion table and function definitions 
//----------------------------------------------------------------------------
typedef struct enum_name_val_pair {
  char * name;
  int val;
} ENUM_NAME_VAL_PAIR;

typedef struct enum_table {
  int multiple;
  char * delim;
  int nv_count;
  ENUM_NAME_VAL_PAIR * nv_by_name;
  ENUM_NAME_VAL_PAIR * nv_by_val;
} ENUM_TABLE;

#define ENUM_START(etname) ENUM_NAME_VAL_PAIR etname##__name_val[] = {
#define ENUM_NAME(name, value) { name, value },
#define ENUM_NAMP(prefix, name) { #name, prefix##name },
#define ENUM_END(etname, multiple, delim) {NULL} }; ENUM_TABLE etname = {multiple, delim, -1, etname##__name_val};

// Sets *name to point to a string containing the enum name.  Caller must free.
int enum2str(MSG_CONTEXT *msgctx, ENUM_TABLE * etptr, int val, char ** name);

// Sets *val to an enum value or OR of values for multiple types
int str2enum(MSG_CONTEXT *msgctx, ENUM_TABLE * eptr, char * name, int * val);

//----------------------------------------------------------------------------
// hex_dump - dumps size bytes of *data to stdout. Looks like:
// [0000] 75 6E 6B 6E 6F 77 6E 20   30 FF 00 00 00 00 39 00   unknown 0.....9.
//----------------------------------------------------------------------------
void hex_dump(void *data, int size);

#endif
// --- end of cw_misc.h ---
