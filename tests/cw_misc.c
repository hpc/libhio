//============================================================================
// cw_misc.c - various small functions and macros to simplify the writing
// of utility programs.
//============================================================================
#define _GNU_SOURCE
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
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

char *strdupx(MSG_CONTEXT *msgctx, const char *context, const char *s1) {
  char *r = strdup(s1);
  DBG4("strdupx %s \"%s\" returns %p", context, s1, r);
  if (!r) ERRX("%s: strdup() failed", context)
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
      *name = STRDUPX(nvp->name);
      rc = 0;
    } else {
      char msg[32];
      snprintf(msg, sizeof(msg), "INVALID(0x%X)", val);
      *name = STRDUPX(msg);
      rc = -1;
    }
  }
  return rc;
}

// Returns a pointer to a string containing the enum name. Not valid for multiple.
char * enum_name(MSG_CONTEXT *msgctx, ENUM_TABLE * etptr, int val) {
  char * retval = "<Invalid_Enum>";
  if (etptr->multiple) ERRX("enum_name called for enum type multiple");

  // nv_count < 0 is a a flag that the table is unsorted.  Count and sort the table.
  if (etptr->nv_count < 0) enum_table_sort(msgctx, etptr);

  ENUM_NAME_VAL_PAIR nv = {NULL, val};
  ENUM_NAME_VAL_PAIR *nvp = bsearch(&nv, etptr->nv_by_val, etptr->nv_count, sizeof(ENUM_NAME_VAL_PAIR), enum_val_compare);
  if (nvp) {
    retval = nvp->name;
  }
  return retval;
}

int str2enum(MSG_CONTEXT *msgctx, ENUM_TABLE * etptr, char * name, int * val) {
  int rc = 0;
  // nv_count < 0 is a a flag that the table is unsorted.  Count and sort the table.
  if (etptr->nv_count < 0) enum_table_sort(msgctx, etptr);

  if (etptr->multiple) {
    char * string = STRDUPX(name);
    char * token, *saveptr;
    int myval = 0;
    
    token = strtok_r(string, etptr->delim, &saveptr);
    while (token) {
    ENUM_NAME_VAL_PAIR nv = {token, 0};
      ENUM_NAME_VAL_PAIR *nvp = bsearch(&nv, etptr->nv_by_name, etptr->nv_count, sizeof(ENUM_NAME_VAL_PAIR), enum_name_compare);
      if (nvp) {
        myval |= nvp->val;
      } else {
        rc = -1;
      } 
      token = strtok_r(NULL, etptr->delim, &saveptr);
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

// Returns a list of enum names prefixed by "one of" or "one or more of".  List 
// must be freed by caller.
char * enum_list(MSG_CONTEXT *msgctx, ENUM_TABLE * etptr) {
  // nv_count < 0 is a a flag that the table is unsorted.  Count and sort the table.
  if (etptr->nv_count < 0) enum_table_sort(msgctx, etptr);
  
  char * retval = STRDUPX(etptr->multiple?"one or more of ": "one of ");
  for (int i = 0; i < etptr->nv_count; i++) {
    retval = STRCATRX(retval, etptr->nv_by_name[i].name);
    retval = STRCATRX(retval, ", "); 
  }
  retval[strlen(retval)-2] = '\0';
  return retval;
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

//----------------------------------------------------------------------------
// Simple timer start/stop routines - returns floating point seconds
//----------------------------------------------------------------------------
void etimer_start(MSG_CONTEXT *msgctx, const char *context, ETIMER * timerp) {
  #if defined(CLOCK_REALTIME) && defined(USE_REALTIME)
    if (clock_gettime(CLOCK_REALTIME, &timerp->start)) ERRX("clock_gettime() failed: %s", strerror(errno));
  #else
    // Silly Mac OS doesn't support clock_gettime :-(
    if (gettimeofday(&timerp->start, NULL)) ERRX("gettimeofday() failed: %s", strerror(errno));
  #endif
}

double etimer_elapsed(MSG_CONTEXT *msgctx, const char *context, ETIMER * timerp) {
  #if defined(CLOCK_REALTIME) && defined(USE_REALTIME)
    if (clock_gettime(CLOCK_REALTIME, &timerp->end)) ERRX("clock_gettime() failed: %s", strerror(errno));
    return (double)(timerp->end.tv_sec - timerp->start.tv_sec) +
             (1E-9 * (double) (timerp->end.tv_nsec - timerp->start.tv_nsec));
  #else
    if (gettimeofday(&timerp->end, NULL)) ERRX("gettimeofday() failed: %s", strerror(errno));
    return (double)(timerp->end.tv_sec - timerp->start.tv_sec) +
             (1E-6 * (double) (timerp->end.tv_usec - timerp->start.tv_usec));
  #endif
}

// Sleep for floating point seconds and fractions
void fsleep(double seconds) {
  double seconds_only = floor(seconds);
  double microseconds = 1000000 * (seconds - seconds_only);

  sleep((int) seconds);
  usleep((int) microseconds);
}

/*-
 *  COPYRIGHT (C) 1986 Gary S. Brown.  You may use this program, or
 *  code or tables extracted from it, as desired without restriction.
 *
 *  First, the polynomial itself and its table of feedback terms.  The
 *  polynomial is
 *  X^32+X^26+X^23+X^22+X^16+X^12+X^11+X^10+X^8+X^7+X^5+X^4+X^2+X^1+X^0
 *
 *  Note that we take it "backwards" and put the highest-order term in
 *  the lowest-order bit.  The X^32 term is "implied"; the LSB is the
 *  X^31 term, etc.  The X^0 term (usually shown as "+1") results in
 *  the MSB being 1
 *
 *  Note that the usual hardware shift register implementation, which
 *  is what we're using (we're merely optimizing it by doing eight-bit
 *  chunks at a time) shifts bits into the lowest-order term.  In our
 *  implementation, that means shifting towards the right.  Why do we
 *  do it this way?  Because the calculated CRC must be transmitted in
 *  order from highest-order term to lowest-order term.  UARTs transmit
 *  characters in order from LSB to MSB.  By storing the CRC this way
 *  we hand it to the UART in the order low-byte to high-byte; the UART
 *  sends each low-bit to hight-bit; and the result is transmission bit
 *  by bit from highest- to lowest-order term without requiring any bit
 *  shuffling on our part.  Reception works similarly
 *
 *  The feedback terms table consists of 256, 32-bit entries.  Notes
 *
 *      The table can be generated at runtime if desired; code to do so
 *      is shown later.  It might not be obvious, but the feedback
 *      terms simply represent the results of eight shift/xor opera
 *      tions for all combinations of data and CRC register values
 *
 *      The values must be right-shifted by eight bits by the "updcrc
 *      logic; the shift must be unsigned (bring in zeroes).  On some
 *      hardware you could probably optimize the shift in assembler by
 *      using byte-swap instructions
 *      polynomial $edb88320
 *
 *
 * CRC32 code derived from work by Gary S. Brown.
 */

// #include <sys/param.h>
// #include <sys/systm.h>

static unsigned int crc32_tab[] = {
	0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
	0xe963a535, 0x9e6495a3,	0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
	0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
	0xf3b97148, 0x84be41de,	0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
	0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec,	0x14015c4f, 0x63066cd9,
	0xfa0f3d63, 0x8d080df5,	0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
	0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,	0x35b5a8fa, 0x42b2986c,
	0xdbbbc9d6, 0xacbcf940,	0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
	0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
	0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
	0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,	0x76dc4190, 0x01db7106,
	0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
	0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
	0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
	0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
	0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
	0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
	0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
	0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
	0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
	0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
	0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
	0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
	0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
	0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
	0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
	0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
	0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
	0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
	0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
	0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
	0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
	0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
	0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
	0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
	0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
	0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
	0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
	0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
	0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
	0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
	0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
	0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

unsigned int crc32(unsigned int crc, const void *buf, size_t size) {
	const unsigned char *p;

	p = buf;
	crc = crc ^ ~0U;

	while (size--) {
	  crc = crc32_tab[(crc ^ *p++) & 0xFF] ^ (crc >> 8);
	  //crc = *p++;
        }
	return crc ^ ~0U;
}

//----------------------------------------------------------------------------
// memdiff - compares two memory areas, returns NULL if they match, else the 
// address in the first operand where the first differing byte occurs.
//----------------------------------------------------------------------------
void * memdiff(const void * s1, const void *s2, size_t n) {
  char * c1 = (char *) s1;
  char * c2 = (char *) s2;
  for (size_t i = 0; i<n; i++) {
    if ( *(c1+i) != *(c2+i) ) {
      return (void *)(c1+i);
    }
  }
  return NULL;
}

// --- end of cw_misc.c ---
