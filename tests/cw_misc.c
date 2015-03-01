//============================================================================
// cw_misc.c - various small functions and macros to simplify the writing
// of utility programs.
//============================================================================
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include "cw_misc.h"

//----------------------------------------------------------------------------
// Message control functions
//----------------------------------------------------------------------------
void msg_context_init(MSG_CONTEXT *msgctx, int verbose_level, int debug_level) {
  msgctx->id_string = "";
  msgctx->verbose_level = verbose_level;
  msgctx->debug_level = debug_level;
  msgctx->std_file = stdout;
  msgctx->err_file = stderr;
}

void msg_context_set_verbose(MSG_CONTEXT *msgctx, int verbose_level) {
  msgctx->verbose_level = verbose_level;
}

void msg_context_set_debug(MSG_CONTEXT *msgctx, int debug_level) {
  msgctx->debug_level = debug_level;
}

void msg_context_free(MSG_CONTEXT *msgctx) {}

void msg_writer(MSG_CONTEXT *msgctx, FILE * stream, const char *format, ...) {
  va_list args;
  char msg_buf[1024];
  char time_str[64];
  time_t now;

  now = time(0);
  strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&now));

  va_start(args, format);
  vsnprintf(msg_buf, sizeof(msg_buf), format, args);
  va_end(args); 
  fprintf(stream, "%s %s%s\n", time_str, msgctx->id_string, msg_buf);
}

//----------------------------------------------------------------------------
// Wrappers for functions that manage memory which will check results and
// call ERRX on failure.
//----------------------------------------------------------------------------
#define MY_MSG_CTX msgctx

void *mallocx(MSG_CONTEXT *msgctx, const char *context, size_t size) {
  void *r = malloc(size);
  DBG4("mallocx %s returns %p", context, r);
  if (!r) ERRX("%s: malloc(%d) failed", context, size);
  return r;
}

void *reallocx(MSG_CONTEXT *msgctx, const char *context, void *ptr, size_t size) {
  void *r = realloc(ptr, size);
  DBG4("reallocx %s old: %p new: %p", context, ptr, r);
  if (!r) ERRX("%s: realloc(%d) failed", context, size);
  return r;
}

void *freex(MSG_CONTEXT *msgctx, const char *context, void *ptr) {
  DBG4("freex %s %p", context, ptr);
  free(ptr);
  return NULL;
}

char *strndupx(MSG_CONTEXT *msgctx, const char *context, const char *s1, size_t n) {
  char *r = strndup(s1, n);
  DBG4("strndupx %s \"%s\" returns %p", context, s1, r);
  if (!r) ERRX("%s: strndup(%d) failed", context, n)
  return r;
}

char *strcatrx(MSG_CONTEXT *msgctx, const char *context, const char *s1, const char *s2) {
  size_t newsize = ((s1) ? strlen(s1): 0) + strlen(s2) + 1;
  char *r = reallocx(msgctx, context, (void *)s1, newsize);
  if (!s1) *r = '\0';
  strcat(r, s2);
  DBG4("strcatr %s returns %p", context, r);
  return r;
}

char *alloc_printf(MSG_CONTEXT *msgctx, const char *context, const char *format, ...) {
  va_list args;
  #define BUFLEN 1024
  char *msg_buf;
  size_t msg_len;

  msg_buf = mallocx(msgctx, "alloc_printf", BUFLEN);
  va_start(args, format);
  msg_len = 1 + vsnprintf(msg_buf, BUFLEN, format, args);
  va_end(args); 
  msg_buf = reallocx(msgctx, "alloc_printf", msg_buf, msg_len);
  return msg_buf;
}

//----------------------------------------------------------------------------
// Enum name/value conversion table and function definitions 
//----------------------------------------------------------------------------
int enum_name_compare(const void * nv1, const void * nv2) {
  return strcmp((*(ENUM_NAME_VAL_PAIR*)nv1).name, (*(ENUM_NAME_VAL_PAIR*)nv2).name);
} 

int enum_val_compare(const void * nv1, const void * nv2) {
  int v1 = (*(ENUM_NAME_VAL_PAIR*)nv1).val;
  int v2 = (*(ENUM_NAME_VAL_PAIR*)nv2).val;

  return (v1 == v2) ? 0: ( (v1 > v2) ? 1: -1 );
} 

void enum_table_sort(MSG_CONTEXT *msgctx, ENUM_TABLE * etptr) {
  ENUM_NAME_VAL_PAIR * nvn = etptr->nv_by_name; 
  ENUM_NAME_VAL_PAIR * nvv = etptr->nv_by_val; 
  int i, n = etptr->nv_count;

  // Count entries
  i = -1;
  while ( nvn[++i].name );
  etptr->nv_count = n = i;
  
  // Sort by name
  qsort(nvn, n, sizeof(ENUM_NAME_VAL_PAIR), enum_name_compare);

  // Create second copy of table, sort by value
  nvv = etptr->nv_by_val = MALLOCX(n * sizeof(ENUM_NAME_VAL_PAIR) );
  memcpy(nvv, nvn, n * sizeof(ENUM_NAME_VAL_PAIR) );
  qsort(nvv, n, sizeof(ENUM_NAME_VAL_PAIR), enum_val_compare);
}

int enum2str(MSG_CONTEXT *msgctx, ENUM_TABLE * etptr, int val, char ** name) {
  int rc;

  // nv_count < 0 is a a flag that the table is unsorted.  Count and sort the table.
  if (etptr->nv_count < 0) enum_table_sort(msgctx, etptr);

  if (etptr->multiple) {
    char * str = NULL;    
    int part = val;
    DBG4("part: 0x%X  str: %s", part, str);
    for (int i = etptr->nv_count-1; i >= 0; i--) {
      if ((~part & etptr->nv_by_val[i].val) == 0) {
        if (str) str = STRCATRX(str, etptr->delim);
        str = STRCATRX(str, etptr->nv_by_val[i].name);
        part &= ~etptr->nv_by_val[i].val;
        DBG4("val: 0x%X  part: 0x%X  str: %s", etptr->nv_by_val[i].val, part, str);
        if (part == 0) break;
      } 
    } 
    DBG4("part: 0x%X  str: %s", part, str);
    if (part != 0) {
      char msg[32];
      if (str) str = STRCATRX(str, etptr->delim);
      snprintf(msg, sizeof(msg), "INVALID(0x%X)", part);
      str = STRCATRX(str, msg);
      rc = -1;
    } else {
      rc = 0;
    }
    *name = str;
  } else {
    ENUM_NAME_VAL_PAIR nv = {NULL, val};
    ENUM_NAME_VAL_PAIR *nvp = bsearch(&nv, etptr->nv_by_val, etptr->nv_count, sizeof(ENUM_NAME_VAL_PAIR), enum_val_compare);
    if (nvp) {
      *name = STRNDUPX(nvp->name, 64);
      rc = 0;
    } else {
      char msg[32];
      snprintf(msg, sizeof(msg), "INVALID(0x%X)", val);
      *name = STRNDUPX(msg, sizeof(msg));
      rc = -1;
    }
  }
  return rc;
}

int str2enum(MSG_CONTEXT *msgctx, ENUM_TABLE * etptr, char * name, int * val) {
  int rc = 0;
  // nv_count < 0 is a a flag that the table is unsorted.  Count and sort the table.
  if (etptr->nv_count < 0) enum_table_sort(msgctx, etptr);

  if (etptr->multiple) {
    char * string = STRNDUPX(name, 1000);
    char * token;
    int myval = 0;
    while ((token = strsep(&string, etptr->delim))) {
    ENUM_NAME_VAL_PAIR nv = {token, 0};
      ENUM_NAME_VAL_PAIR *nvp = bsearch(&nv, etptr->nv_by_name, etptr->nv_count, sizeof(ENUM_NAME_VAL_PAIR), enum_name_compare);
      if (nvp) {
        myval |= nvp->val;
      } else {
        rc = -1;
      } 
    }
    if (rc == 0) *val = myval;  
  } else {
    ENUM_NAME_VAL_PAIR nv = {name, 0};
    ENUM_NAME_VAL_PAIR *nvp = bsearch(&nv, etptr->nv_by_name, etptr->nv_count, sizeof(ENUM_NAME_VAL_PAIR), enum_name_compare);
    if (nvp) {
      *val = nvp->val;
    } else {
      rc = -1;
    } 
  }
  return rc;
}

//----------------------------------------------------------------------------
// hex_dump - dumps size bytes of *data to stdout. Looks like:
// [0000] 75 6E 6B 6E 6F 77 6E 20   30 FF 00 00 00 00 39 00   unknown 0.....9.
//----------------------------------------------------------------------------
void hex_dump(void *data, int size) {
    unsigned char *p = data;
    unsigned char c;
    int n;
    char bytestr[4] = {0};
    char addrstr[10] = {0};
    char hexstr[ 16*3 + 5] = {0};
    char hexprev[ 16*3 + 5] = {0};
    char charstr[16*1 + 5] = {0};
    int skipped = 0; 
    for(n=1;n<=size;n++) {
        if (n%16 == 1) {
            /* store address for this line */
            snprintf(addrstr, sizeof(addrstr), "%.4lx", p-(unsigned char *)data);
        }
            
        c = *p;
        if (isalnum(c) == 0) {
            c = '.';
        }

        /* store hex str (for left side) */
        snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
        strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1);

        /* store char str (for right side) */
        snprintf(bytestr, sizeof(bytestr), "%c", c);
        strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);

        if(n%16 == 0) { 
            /* line completed */
            if (!strcmp(hexstr, hexprev) && n< size) { 
              skipped++;
            } else {
              if (skipped > 0) {
                printf("        %d identical lines skipped\n", skipped);
                skipped = 0;
              }
              printf("[%4.4s]   %-50.50s  %s\n", addrstr, hexstr, charstr);
              strcpy(hexprev, hexstr);
            } 
            hexstr[0] = 0;
            charstr[0] = 0;
        } else if(n%8 == 0) {
            /* half line: add whitespaces */
            strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
            strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
        }
        p++; /* next byte */
    }

    if (strlen(hexstr) > 0) {
        if (skipped > 0) {
           printf("        %d identical lines skipped\n", skipped);
           skipped = 0;
        }
        /* print rest of buffer if not empty */
        printf("[%4.4s]   %-50.50s  %s\n", addrstr, hexstr, charstr);
    }
}

// --- end of cw_misc.c ---
