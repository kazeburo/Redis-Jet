#ifdef __cplusplus
extern "C" {
#endif

#define PERL_NO_GET_CONTEXT /* we want efficiency */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>
#include <poll.h>
#include <perlio.h>

#ifdef __cplusplus
} /* extern "C" */
#endif

#define NEED_newSVpvn_flags
#include "ppport.h"

#ifndef STATIC_INLINE /* a public perl API from 5.13.4 */
#   if defined(__GNUC__) || defined(__cplusplus) || (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L))
#       define STATIC_INLINE static inline
#   else
#       define STATIC_INLINE static
#   endif
#endif /* STATIC_INLINE */

#define READ_MAX 16384
#define REQUEST_BUF_SIZE 4096

#define PIPELINE(a) a == 1
#define FIGURES(a) (a==0) ? 1 : (int)log10(a) + 1

struct jet_response_st {
  SV * data;
};

struct redis_jet_s {
  SV * server;
  double connect_timeout;
  double io_timeout;
  int utf8;
  int noreply;
  int reconnect_attempts;
  double reconnect_delay;
  int fileno;
  HV * bucket;
  char * request_buf;
  char * read_buf;
  long int request_buf_len;
  long int read_buf_len;
  struct jet_response_st * response_st;
  long int response_st_len;
};
typedef struct redis_jet_s Redis_Jet;


STATIC_INLINE
int
hv_fetch_iv(pTHX_ HV * hv, const char * key, const int defaultval ) {
  SV **ssv;
  ssv = hv_fetch(hv, key, strlen(key), 0);
  if (ssv) {
    return SvIV(*ssv);
  }
  return defaultval;
}

STATIC_INLINE
double
hv_fetch_nv(pTHX_ HV * hv, const char * key, const double defaultval ) {
  SV **ssv;
  ssv = hv_fetch(hv, key, strlen(key), 0);
  if (ssv) {
    return SvNV(*ssv);
  }
  return defaultval;
}

STATIC_INLINE
void
memcat( char * dst, ssize_t *dst_len, const char * src, const ssize_t src_len ) {
    ssize_t i;
    ssize_t dlen = *dst_len;
    for ( i=0; i<src_len; i++) {
        dst[dlen++] = src[i];
    }
    *dst_len = dlen;
}

STATIC_INLINE
char *
svpv2char(pTHX_ SV *string, STRLEN *len, const int utf8) {
    char *str;
    STRLEN str_len;
    if ( utf8 == 1 ) {
        SvGETMAGIC(string);
        if (!SvUTF8(string)) {
            string = sv_mortalcopy(string);
            sv_utf8_encode(string);
        }
    }
    str = (char *)SvPV(string,str_len);
    *len = str_len;
    return str;
}



STATIC_INLINE
void
renewmem(pTHX_ char **d, ssize_t *cur, const ssize_t req) {
    if ( req > *cur ) {
        *cur = req - (req % 4096) + 4096;
        Renew(*d, *cur, char);
    }
}

STATIC_INLINE
void
memcat_i(char * dst, ssize_t *dst_len, ssize_t snum, const int fig ) {
    int dlen = *dst_len + fig - 1;
    do {
        dst[dlen] = '0' + (snum % 10);
        dlen--;
    } while ( snum /= 10);
    *dst_len += fig;
}

STATIC_INLINE
long int
_index_crlf(const char * buf, const ssize_t buf_len, ssize_t offset) {
  ssize_t ret = -1;
  while( offset < buf_len -1 ) {
    if (buf[offset] == 13 && buf[offset+1] == 10 ) {
      ret = offset;
      break;
    }
    offset++;
  }
  return ret;
}

STATIC_INLINE
void
_av_push(pTHX_ AV * data_av, const char * buf, const ssize_t copy_len, const int utf8) {
    SV * dst;
    dst = newSVpvn(buf, copy_len);
    SvPOK_only(dst);
    if ( utf8 ) { SvUTF8_on(dst); }
    (void)av_push(data_av, dst);
}

STATIC_INLINE
void
_sv_store(pTHX_ SV * data_sv, char * buf, ssize_t copy_len, int utf8) {
    char * d;
    ssize_t i;
    ssize_t dlen = 0;
    d = SvGROW(data_sv, copy_len);
    for (i=0; i<copy_len; i++){
      d[dlen++] = buf[i];
    }
    SvCUR_set(data_sv, dlen);
    SvPOK_only(data_sv);
    if ( utf8 ) {
      SvUTF8_on(data_sv);
    }
}

/*
  == -2 incomplete
  == -1 broken
*/
STATIC_INLINE
long int
_parse_message(pTHX_ char * buf, const ssize_t buf_len, SV * data_sv, SV * error_sv, const int utf8) {
  long int first_crlf;
  long int m_first_crlf;
  ssize_t v_size;
  ssize_t m_size;
  ssize_t m_v_size;
  ssize_t m_buf_len;
  ssize_t m_read;
  ssize_t j;
  char * m_buf;
  AV * av_list;

  if ( buf_len < 2 ) {
    return -2;
  }
  first_crlf = _index_crlf(buf,buf_len,0);
  if ( first_crlf < 0 ) {
    return -2;
  }

  switch ( buf[0] ) {
    case '+':
    case ':':
      /* 1 line reply
      +foo\r\n */
      _sv_store(aTHX_ data_sv, &buf[1], first_crlf-1, utf8);
      return first_crlf + 2;
    case '-':
      /* error
      -ERR unknown command 'a' */
      sv_setsv(data_sv, &PL_sv_undef);
      _sv_store(aTHX_ error_sv, &buf[1], first_crlf-1, utf8);
      return first_crlf + 2;
     case '$':
      /* bulf
        C: get mykey
        S: $3
        S: foo
      */
      if ( buf[1] == '-' && buf[2] == '1' ) {
        sv_setsv(data_sv, &PL_sv_undef);
        sv_setsv(error_sv, &PL_sv_undef);
        return first_crlf + 2;
      }
      v_size = 0;
      for (j=1; j<first_crlf; j++ ) {
        v_size = v_size * 10 + (buf[j] - '0');
      }
      if ( buf_len - (first_crlf + 2) < v_size + 2 ) {
        return -2;
      }
      _sv_store(aTHX_ data_sv, &buf[first_crlf+2], v_size, utf8);
      sv_setsv(error_sv, &PL_sv_undef);
      return first_crlf+2+v_size+2;
    case '*':
      /* multibulk
         # *3
         # $3
         # foo
         # $-1
         # $3
         # baa
         #
         ## null list/timeout
         # *-1
         #
      */
      if ( buf[1] == '-' && buf[2] == '1' ) {
        sv_setsv(data_sv, &PL_sv_undef);
        sv_setsv(error_sv, &PL_sv_undef);
        return first_crlf + 2;
      }
      m_size = 0;
      for (j=1; j<first_crlf; j++ ) {
        m_size = m_size * 10 + (buf[j] - '0');
      }
      av_list = newAV();
      if ( m_size == 0 ) {
        sv_setsv(data_sv, sv_2mortal(newRV_noinc((SV *) av_list)));
        sv_setsv(error_sv, &PL_sv_undef);
        return first_crlf + 2;
      }
      m_buf = &buf[first_crlf + 2];
      m_buf_len = buf_len - (first_crlf + 2);
      m_read = 0;
      while ( m_buf_len > m_read ) {
        if (m_buf[0] != '$' ) {
          return -1;
        }
        if (m_buf[1] == '-' && m_buf[2] == '1' ) {
          av_push(av_list, &PL_sv_undef);
          m_buf += 5;
          m_read += 5;
          continue;
        }
        m_first_crlf = _index_crlf(m_buf, m_buf_len - m_read, 0);
        if ( m_first_crlf < 0 ) {
          return -2;
        }
        m_v_size = 0;
        for (j=1; j<m_first_crlf; j++ ) {
          m_v_size = m_v_size * 10 + (m_buf[j] - '0');
        }
        if ( m_buf_len - m_read - (m_first_crlf + 2) < m_v_size + 2 ) {
          return -2;
        }
        _av_push(aTHX_ av_list, &m_buf[m_first_crlf+2], m_v_size, utf8);
        m_buf += m_first_crlf+2+m_v_size+2;
        m_read += m_first_crlf+2+m_v_size+2;
        if ( av_len(av_list) + 1 == m_size ) {
          break;
        }
      }
      if ( av_len(av_list) + 1 < m_size ) {
        return -2;
      }
      sv_setsv(data_sv, sv_2mortal(newRV_noinc((SV *) av_list)));
      sv_setsv(error_sv, &PL_sv_undef);
      return first_crlf + 2 + m_read;
    default:
      return -1;
  }
}

STATIC_INLINE
ssize_t
_write_timeout(const int fileno, const double timeout, char * write_buf, const int write_len ) {
    int rv;
    int nfound;
    struct pollfd wfds[1];
  DO_WRITE:
    rv = write(fileno, write_buf, write_len);
    if ( rv >= 0 ) {
      return rv;
    }
    if ( rv < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK ) {
      return rv;
    }
  WAIT_WRITE:
    while (1) {
      wfds[0].fd = fileno;
      wfds[0].events = POLLOUT;
      nfound = poll(wfds, 1, (int)timeout*1000);
      if ( nfound == 1 ) {
        break;
      }
      if ( nfound == 0 && errno != EINTR ) {
        return -1;
      }
    }
    goto DO_WRITE;
}


STATIC_INLINE
ssize_t
_read_timeout(const int fileno, const double timeout, char * read_buf, const int read_len ) {
    int rv;
    int nfound;
    struct pollfd rfds[1];
  DO_READ:
    rfds[0].fd = fileno;
    rfds[0].events = POLLIN;
    rv = read(fileno, read_buf, read_len);
    if ( rv >= 0 ) {
      return rv;
    }
    if ( rv < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK ) {
      return rv;
    }
  WAIT_READ:
    while (1) {
      nfound = poll(rfds, 1, (int)timeout*1000);
      if ( nfound == 1 ) {
        break;
      }
      if ( nfound == 0 && errno != EINTR ) {
        return -1;
      }
    }
    goto DO_READ;
}

STATIC_INLINE
void
disconnect_socket (pTHX_ Redis_Jet * self) {
  self->fileno = 0;
  if ( hv_exists(self->bucket, "socket", strlen("socket")) ) {
    (void)hv_delete(self->bucket, "socket", strlen("socket"), 0);
  }
}

MODULE = Redis::Jet    PACKAGE = Redis::Jet

PROTOTYPES: DISABLE

Redis_Jet *
_new(class, args)
    char * class
    SV * args
  PREINIT:
    Redis_Jet * self;
    STRLEN server_len;
    char * s;
    SV **server_ssv;
  CODE:
    Newxz(self, sizeof(Redis_Jet), Redis_Jet);
    if ( SvTYPE(SvRV(args)) == SVt_PVHV) {
      server_ssv = hv_fetch((HV *)SvRV(args), "server", strlen("server"),0);
      if ( server_ssv ) {
        self->server = newSVsv(*server_ssv);
      }
      else {
        self->server = newSVpvs("127.0.0.1:6379");
      }
      self->utf8 = hv_fetch_iv(aTHX_ (HV *)SvRV(args), "utf8", 0);
      self->connect_timeout = hv_fetch_nv(aTHX_ (HV *)SvRV(args), "connect_timeout", 10);
      self->io_timeout = hv_fetch_nv(aTHX_ (HV *)SvRV(args), "io_timeout", 10);
      self->noreply = hv_fetch_iv(aTHX_ (HV *)SvRV(args), "noreply", 0);
      self->reconnect_attempts = hv_fetch_iv(aTHX_ (HV *)SvRV(args), "reconnect_attempts", 0);
      self->reconnect_delay = hv_fetch_nv(aTHX_ (HV *)SvRV(args), "reconnect_delay", 10);
      self->bucket = newHV();
    }
    else {
      croak("Not a hash reference");
    }
    self->request_buf_len = 0;
    self->read_buf_len = 0;
    self->response_st_len = 0;
    RETVAL = self;
  OUTPUT:
    RETVAL

HV *
get_bucket(self)
    Redis_Jet * self
  CODE:
    RETVAL = self->bucket;
  OUTPUT:
    RETVAL

SV *
get_server(self)
    Redis_Jet * self
  PREINIT:
  PPCODE:
    XPUSHs(self->server);


double
get_connect_timeout(self)
    Redis_Jet * self
  CODE:
    RETVAL = self->connect_timeout;
  OUTPUT:
    RETVAL

double
get_io_timeout(self)
    Redis_Jet * self
  CODE:
    RETVAL = self->io_timeout;
  OUTPUT:
    RETVAL

int
get_utf8(self)
    Redis_Jet * self
  CODE:
    RETVAL = self->utf8;
  OUTPUT:
    RETVAL

int
get_noreply(self)
    Redis_Jet * self
  CODE:
    RETVAL = self->noreply;
  OUTPUT:
    RETVAL

int
set_fileno(self,fileno)
    Redis_Jet * self
    int fileno
  CODE:
    RETVAL = self->fileno = fileno;
  OUTPUT:
    RETVAL

SV *
_destroy(self)
    Redis_Jet * self
  CODE:
    if ( self->request_buf_len != 0 ) {
      Safefree(self->request_buf);
    }
    if ( self->response_st_len != 0 ) {
      Safefree(self->response_st);
    }
    if ( self->read_buf_len != 0 ) {
      Safefree(self->read_buf);
    }
    disconnect_socket(self);
    SvREFCNT_dec((SV*)self->server);
    SvREFCNT_dec((SV*)self->bucket);
    Safefree(self);

SV *
parse_message(buf_sv, res_av)
    SV * buf_sv
    AV * res_av
  ALIAS:
    Redis::Jet::parse_message = 0
    Redis::Jet::parse_message_utf8 = 1
  PREINIT:
    ssize_t buf_len;
    char * buf;
    AV * data_av;
    SV * data_sv;
    SV * error_sv;
    long int ret;
    long int readed;
  CODE:
    buf_len = SvCUR(buf_sv);
    buf = SvPV_nolen(buf_sv);
    readed = 0;
    while ( buf_len > 0 ) {
      data_sv = newSV(0);
      (void)SvUPGRADE(data_sv, SVt_PV);
      error_sv = newSV(0);
      (void)SvUPGRADE(error_sv, SVt_PV);
      ret = _parse_message(aTHX_ buf, buf_len, data_sv, error_sv, ix);
      if ( ret == -1 ) {
        XSRETURN_UNDEF;
      }
      else if ( ret == -2 ) {
        break;
      }
      else {
        data_av = newAV();
        av_push(data_av, data_sv);
        if ( SvOK(error_sv) ) {
          av_push(data_av, error_sv);
        } else {
          sv_2mortal(error_sv);
        }
        av_push(res_av, newRV_noinc((SV *) data_av));
        readed += ret;
        buf_len -= ret;
        buf = &buf[ret];
      }
    }
    RETVAL = newSViv(readed);
  OUTPUT:
    RETVAL

SV *
command(self,...)
    Redis_Jet * self
  ALIAS:
    Redis::Jet::command = 0
    Redis::Jet::pipeline = 1
  PREINIT:
    AV * data_av;
    SV * data_sv;
    SV * error_sv;
    ssize_t i, j;
    long int ret;
    /* build */
    int args_offset = 1;
    int fig;
    int connect_retry = 0;
    ssize_t pipeline_len = 1;
    ssize_t request_len = 0;
    STRLEN request_arg_len;
    char * request_arg;
    AV * request_arg_list;
    /* send */
    ssize_t written;
    char * write_buf;
    /* response */
    ssize_t readed;
    ssize_t parse_offset;
    ssize_t parsed_response;
    long int parse_result;
  PPCODE:
    /* init */
    if ( self->request_buf_len == 0 ) {
      Newx(self->request_buf, REQUEST_BUF_SIZE, char);
      self->request_buf_len = REQUEST_BUF_SIZE;
    }
    if ( self->read_buf_len == 0 ) {
      Newx(self->read_buf, READ_MAX, char);
      self->read_buf_len = READ_MAX;
    }
    if ( self->response_st_len == 0 ) {
      Newx(self->response_st, sizeof(struct jet_response_st)*10, struct jet_response_st);
      self->response_st_len = 30;
    }
    DO_CONNECT:
    /* connect */
    if ( self->fileno == 0 ) {
      {
        dSP;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);
        XPUSHs(ST(0));
        PUTBACK;
        call_method("connect", G_DISCARD);
        FREETMPS;
        LEAVE;
      }
      if ( self->fileno == 0 ) {
        /* connection error */
        disconnect_socket(self);
        if ( self->reconnect_attempts > 0 && self->reconnect_attempts > connect_retry ) {
          connect_retry++;
          usleep(self->reconnect_delay*1000000); // micro-sec
          goto DO_CONNECT;
        }
        if ( PIPELINE(ix) ) {
          pipeline_len = items - args_offset;
          EXTEND(SP, pipeline_len);
          for (i=0; i<pipeline_len; i++) {
            data_av = newAV();
            (void)av_push(data_av, &PL_sv_undef);
            (void)av_push(data_av, newSVpvf("failed to connect server: %s",
              ( errno != 0 ) ? strerror(errno) : "timeout"));
            PUSHs( sv_2mortal(newRV_noinc((SV *) data_av)) );
          }
        }
        else {
          EXTEND(SP, 2);
          PUSHs(&PL_sv_undef);
          PUSHs(sv_2mortal(newSVpvf("failed to connect server: %s",
              ( errno != 0 ) ? strerror(errno) : "timeout")));
        }
        goto COMMAND_DONE;
      }
    }

    /* connection successful */
    connect_retry = 0;

    // char * s = SvPV_nolen(ST(1));
    // printf("ix:%d,fileno:%d,utf8:%d,timeout:%f,noreply:%d, items:%d %s\n",ix,self->fileno,self->utf8,self->io_timeout,self->noreply,items,&s[0]);


    /* build_message */
    if ( PIPELINE(ix) ) {
      /* build_request([qw/set foo bar/],[qw/set bar baz/]) */
      pipeline_len = items - args_offset;
      for( i=args_offset; i < items; i++ ) {
        if ( SvOK(ST(i)) && SvROK(ST(i)) && SvTYPE(SvRV(ST(i))) == SVt_PVAV ) {
          request_arg_list = (AV *)SvRV(ST(i));
          fig = FIGURES(av_len(request_arg_list)+1);
          /* 1(*) + args + 2(crlf)  */
          renewmem(aTHX_ &self->request_buf, &self->request_buf_len, 1 + fig + 2);
          self->request_buf[request_len++] = '*';
          memcat_i(self->request_buf, &request_len, av_len(request_arg_list)+1, fig);
          self->request_buf[request_len++] = 13; // \r
          self->request_buf[request_len++] = 10; // \n
          for (j=0; j<av_len(request_arg_list)+1; j++) {
            request_arg = svpv2char(aTHX_ *av_fetch(request_arg_list,j,0), &request_arg_len, self->utf8);
            fig = FIGURES(request_arg_len);
            /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
            renewmem(aTHX_ &self->request_buf, &self->request_buf_len, 1 + fig + 2 + request_arg_len + 2);
            self->request_buf[request_len++] = '$';
            memcat_i(self->request_buf, &request_len, request_arg_len, fig);
            self->request_buf[request_len++] = 13; // \r
            self->request_buf[request_len++] = 10; // \n
            memcat(self->request_buf, &request_len, request_arg, request_arg_len);
            self->request_buf[request_len++] = 13; // \r
            self->request_buf[request_len++] = 10; // \n
          }
       }
       else {
          /* 1(*) + 1(args) + 2(crlf)  */
          renewmem(aTHX_ &self->request_buf, &self->request_buf_len, 1 + 1 + 2);
          self->request_buf[request_len++] = '*';
          self->request_buf[request_len++] = '1';
          self->request_buf[request_len++] = 13; // \r
          self->request_buf[request_len++] = 10; // \n
          request_arg = svpv2char(aTHX_ ST(i), &request_arg_len, self->utf8);
          fig = FIGURES(request_arg_len);
          /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
          renewmem(aTHX_ &self->request_buf, &self->request_buf_len, 1 + fig + 2 + request_arg_len + 2);
          self->request_buf[request_len++] = '$';
          memcat_i(self->request_buf, &request_len, request_arg_len, fig);
          self->request_buf[request_len++] = 13; // \r
          self->request_buf[request_len++] = 10; // \n
          memcat(self->request_buf, &request_len, request_arg, request_arg_len);
          self->request_buf[request_len++] = 13; // \r
          self->request_buf[request_len++] = 10; // \n
        }
      }
    }
    else {
      /* build_request(qw/set bar baz/) */
      fig = FIGURES(items-args_offset);
      /* 1(*) + fig + 2(crlf)  */
      renewmem(aTHX_ &self->request_buf, &self->request_buf_len, 1 + fig + 2);
      self->request_buf[request_len++] = '*';
      memcat_i(self->request_buf, &request_len, items-args_offset, fig);
      self->request_buf[request_len++] = 13; // \r
      self->request_buf[request_len++] = 10; // \n
      for (j=args_offset; j<items; j++) {
        request_arg = svpv2char(aTHX_ ST(j), &request_arg_len, self->utf8);
        fig = FIGURES(request_arg_len);
        /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
        renewmem(aTHX_ &self->request_buf, &self->request_buf_len, 1 + fig + 2 + request_arg_len + 2);
        self->request_buf[request_len++] = '$';
        memcat_i(self->request_buf, &request_len, request_arg_len, fig);
        self->request_buf[request_len++] = 13; // \r
        self->request_buf[request_len++] = 10; // \n
        memcat(self->request_buf, &request_len, request_arg, request_arg_len);
        self->request_buf[request_len++] = 13; // \r
        self->request_buf[request_len++] = 10; // \n
      }
    }

    // printf("== %s --\n",&self->request_buf[0]);
    // printf("pipeline_len:%d,%d,%ld\n",args_offset,items,pipeline_len);
    /* send request */
    written = 0;
    write_buf = &self->request_buf[0];
    while ( request_len > written ) {
      ret = _write_timeout(self->fileno, self->io_timeout, write_buf, request_len - written);
      if ( ret <= 0 ) {
        break;
      }
      written += ret;
      write_buf = &self->request_buf[written];
    }

    /* request done */
    if ( PIPELINE(ix) ) {
      EXTEND(SP, pipeline_len);
      if ( pipeline_len > self->response_st_len ) {
        Renew(self->response_st, sizeof(struct jet_response_st)*pipeline_len, struct jet_response_st);
        self->response_st_len = pipeline_len;
      }
    }
    else {
      EXTEND(SP, 2);
    }

    /* request error */
    if (ret <= 0) {
      disconnect_socket(self);
      if ( ret == 0 && self->reconnect_attempts > 0 && self->reconnect_attempts > connect_retry ) {
        connect_retry++;
        usleep(self->reconnect_delay*1000000);  // micro-sec
        goto DO_CONNECT;
      }
      if ( PIPELINE(ix) ) {
        for (i=0; i<pipeline_len; i++) {
          data_av = newAV();
          (void)av_push(data_av, &PL_sv_undef);
          (void)av_push(data_av, newSVpvf("failed to send message: %s",
            ( errno != 0 ) ? strerror(errno) : "timeout or disconnected"));
          PUSHs( sv_2mortal(newRV_noinc((SV *) data_av)) );
        }
      }
      else {
        PUSHs(&PL_sv_undef);
        PUSHs(sv_2mortal(newSVpvf("failed to send message: %s",
            ( errno != 0 ) ? strerror(errno) : "timeout or disconnected")));
      }
      goto COMMAND_DONE;
    }

    /* noreply */
    if ( self->noreply > 0 ) {
      ret = read(self->fileno, &self->read_buf[0], READ_MAX);
      if ( PIPELINE(ix) ) {
        for (i=0; i<pipeline_len; i++) {
          data_av = newAV();
          (void)av_push(data_av, newSVpvs("0 but true"));
          PUSHs( sv_2mortal(newRV_noinc((SV *) data_av)) );
        }
      }
      else {
        PUSHs(sv_2mortal(newSVpvs("0 but true")));
      }
      goto COMMAND_DONE;
    }

    /* read response */
    parsed_response=0;
    parse_offset=0;
    readed = 0;
    while (1) {
      ret = _read_timeout(self->fileno, self->io_timeout, &self->read_buf[readed], READ_MAX);
      if ( ret <= 0 ) {
        /* timeout or error */
        disconnect_socket(self);
        if ( PIPELINE(ix) ) {
          for (i=parsed_response; i<pipeline_len; i++) {
            data_av = newAV();
            (void)av_push(data_av, &PL_sv_undef);
            (void)av_push(data_av, newSVpvf("failed to read message: %s", ( errno != 0 ) ? strerror(errno) : "timeout or disconnected"));
            self->response_st[i].data = newRV_noinc((SV*)data_av);
          }
        }
        else {
          self->response_st[0].data = &PL_sv_undef;
          self->response_st[1].data = newSVpvf("failed to read message: %s", ( errno != 0 ) ? strerror(errno) : "timeout or disconnected");
        }
        goto PARSER_DONE;
      }
      readed += ret;
      while ( readed > parse_offset ) {
        data_sv = newSV(0);
        (void)SvUPGRADE(data_sv, SVt_PV);
        error_sv = newSV(0);
        (void)SvUPGRADE(error_sv, SVt_PV);
        parse_result = _parse_message(aTHX_ &self->read_buf[parse_offset],
                                            readed - parse_offset, data_sv, error_sv, self->utf8);
        if ( parse_result == -1 ) {
          /* corruption */
          disconnect_socket(self);
          if ( PIPELINE(ix) ) {
            for (i=parsed_response; i<pipeline_len; i++) {
              data_av = newAV();
              (void)av_push(data_av, &PL_sv_undef);
              (void)av_push(data_av, newSVpvs("failed to read message: corrupted message found"));
              self->response_st[i].data = newRV_noinc((SV*)data_av);
            }
          }
          else {
            self->response_st[0].data = &PL_sv_undef;
            self->response_st[1].data = newSVpvs("failed to read message: corrupted message found");
          }
          goto PARSER_DONE;
        }
        else if ( parse_result == -2 ) {
          break;
        }
        else {
          parse_offset += parse_result;
          if ( PIPELINE(ix) ) {
            data_av = newAV();
            av_push(data_av, data_sv);
            if ( SvOK(error_sv) ) {
              av_push(data_av, error_sv);
            }
            else {
              sv_2mortal(error_sv);
            }
            self->response_st[parsed_response++].data = newRV_noinc((SV*)data_av);
            if ( parsed_response >= pipeline_len ) {
              goto PARSER_DONE;
            }
          }
          else {
            self->response_st[0].data = data_sv;
            self->response_st[1].data = error_sv;
            goto PARSER_DONE;
          }
        }
      }
      renewmem(aTHX_ &self->read_buf, &self->read_buf_len, readed + READ_MAX);
    }
    
    PARSER_DONE:
    if ( PIPELINE(ix) ) {
      for (i=0; i<pipeline_len; i++) {
        PUSHs( sv_2mortal((SV *)self->response_st[i].data));
      }
    }
    else {
      PUSHs( sv_2mortal(self->response_st[0].data) );
      if ( SvOK(self->response_st[1].data) ) {
        PUSHs( sv_2mortal(self->response_st[1].data) );
      }
      else {
        sv_2mortal(self->response_st[1].data); /* XXX */
      }
    }
    
    COMMAND_DONE:
    if ( self->request_buf_len > REQUEST_BUF_SIZE * 4 ) {
      Safefree(self->request_buf);
      self->request_buf_len = 0;
    }
    if ( self->response_st_len > 100 ) {
      Safefree(self->response_st);
      self->response_st_len = 0;
    }
    if ( self->read_buf_len > READ_MAX * 4 ) {
      Safefree(self->read_buf);
      self->read_buf_len = 0;
    }

