#ifdef __cplusplus
extern "C" {
#endif

#define PERL_NO_GET_CONTEXT /* we want efficiency */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#ifdef __cplusplus
} /* extern "C" */
#endif

#define NEED_newSVpvn_flags
#include "ppport.h"

struct jet_response_st
{
  AV *data;
};

static
void
memcat( char * dst, ssize_t *dst_len, const char * src, const ssize_t src_len ) {
    ssize_t i;
    ssize_t dlen = *dst_len;
    for ( i=0; i<src_len; i++) {
        dst[dlen++] = src[i];
    }
    *dst_len = dlen;
}

static
void
memcopyset( char * dst, ssize_t dst_len, const char * src, const ssize_t src_len ) {
    ssize_t i;
    ssize_t dlen = dst_len;
    for ( i=0; i<src_len; i++) {
        dst[dlen++] = src[i];
    }
}

static
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

static
void
renewmem(pTHX_ char **d, ssize_t *cur, const ssize_t req) {
    if ( req > *cur ) {
        *cur = ((req % 256) + 1) * 256;
        Renew(*d, *cur, char);
    }
}

static
void
memcat_i(char * dst, ssize_t *dst_len, ssize_t snum ) {
    int dlen = *dst_len;
    do {
        dst[dlen++] = '0' + (snum % 10);
    } while ( snum /= 10);
    *dst_len = dlen;
}

static
long int
_index_crlf(char * buf, const ssize_t buf_len, ssize_t offset) {
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

static
void
_av_push(pTHX_ AV * data_av, const char * buf, const ssize_t copy_len, const int utf8) {
    SV * dst;
    dst = newSVpvn(buf, copy_len);
    SvPOK_only(dst);
    if ( utf8 ) { SvUTF8_on(dst); }
    (void)av_push(data_av, dst);
}

/*
  == -2 incomplete
  == -1 broken
*/
static
long int
_parse_message(pTHX_ char * buf, const ssize_t buf_len, AV * data_av, const int utf8) {
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

  if ( buf[0] == '+' || buf[0] == ':') {
    /* 1 line reply
    +foo\r\n */
    _av_push(aTHX_ data_av, &buf[1], first_crlf-1, utf8);
    return first_crlf + 2;
  }
  else if ( buf[0] == '-' ) {
    /* error
    -ERR unknown command 'a' */
    (void)av_push(data_av, &PL_sv_undef);
    _av_push(aTHX_ data_av, &buf[1], first_crlf-1, utf8);
    return first_crlf + 2;
  }
  else if ( buf[0] == '$' ) {
    /* bulf
       C: get mykey
       S: $3
       S: foo
    */
    if ( buf[1] == '-' && buf[2] == '1' ) {
      (void)av_push(data_av, &PL_sv_undef);
      return first_crlf + 2;
    }
    v_size = 0;
    for (j=1; j<first_crlf; j++ ) {
      v_size = v_size * 10 + (buf[j] - '0');
    }
    if ( buf_len - (first_crlf + 2) < v_size + 2 ) {
      return -2;
    }
    _av_push(aTHX_ data_av, &buf[first_crlf+2], v_size, utf8);
    return first_crlf+2+v_size+2;
  }
  else if ( buf[0] == '*' ) {
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
      (void)av_push(data_av, &PL_sv_undef);
      return first_crlf + 2;
    }
    m_size = 0;
    for (j=1; j<first_crlf; j++ ) {
      m_size = m_size * 10 + (buf[j] - '0');
    }
    av_list = newAV();
    if ( m_size == 0 ) {
      (void)av_push(data_av, newRV_noinc((SV *) av_list));
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
    (void)av_push(data_av, newRV_noinc((SV *) av_list));
    return first_crlf + 2 + m_read;
  }
  else {
    return -1;
  }
}

static
ssize_t
_write_timeout(int fileno, double timeout, char * write_buf, int write_len ) {
    int rv;
    int nfound;
    fd_set wfds;
    struct timeval tv;
    struct timeval tv_start;
    struct timeval tv_end;
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
       FD_ZERO(&wfds);
       FD_SET(fileno, &wfds);
       tv.tv_sec = (int)timeout;
       tv.tv_usec = (timeout - (int)timeout) * 1000000;
       gettimeofday(&tv_start, NULL);
       nfound = select(fileno+1, NULL, &wfds, NULL, &tv);
       gettimeofday(&tv_end, NULL);
       tv.tv_sec = tv_end.tv_sec - tv_start.tv_sec;
       tv.tv_usec = tv_end.tv_usec - tv_start.tv_usec;
       if ( nfound == 1 ) {
         break;
       }
       if ( tv.tv_sec <= 0 && tv.tv_usec <= 0 ) {
         return -1;
       }
    }
    goto DO_WRITE;
}


static
ssize_t
_read_timeout(int fileno, double timeout, char * read_buf, int read_len ) {
    int rv;
    int nfound;
    fd_set rfds;
    struct timeval tv;
    struct timeval tv_start;
    struct timeval tv_end;
  DO_READ:
    rv = read(fileno, read_buf, read_len);
    if ( rv >= 0 ) {
      return rv;
    }
    if ( rv < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK ) {
      return rv;
    }
  WAIT_READ:
    while (1) {
       FD_ZERO(&rfds);
       FD_SET(fileno, &rfds);
       tv.tv_sec = (int)timeout;
       tv.tv_usec = (timeout - (int)timeout) * 1000000;
       gettimeofday(&tv_start, NULL);
       nfound = select(fileno+1, &rfds, NULL, NULL, &tv);
       gettimeofday(&tv_end, NULL);
       tv.tv_sec = tv_end.tv_sec - tv_start.tv_sec;
       tv.tv_usec = tv_end.tv_usec - tv_start.tv_usec;
       if ( nfound == 1 ) {
         break;
       }
       if ( tv.tv_sec <= 0 && tv.tv_usec <= 0 ) {
         return -1;
       }
    }
    goto DO_READ;
}

MODULE = Redis::Jet    PACKAGE = Redis::Jet

PROTOTYPES: DISABLE

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
    long int ret;
    long int readed;
  CODE:
    buf_len = SvCUR(buf_sv);
    buf = SvPV_nolen(buf_sv);
    readed = 0;
    while ( buf_len > 0 ) {
      data_av = newAV();
      ret = _parse_message(aTHX_ buf, buf_len, data_av, ix);
      if ( ret == -1 ) {
        XSRETURN_UNDEF;
      }
      else if ( ret == -2 ) {
        break;
      }
      else {
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
run_command(self,...)
    HV * self
  PREINIT:
    AV * data_av;
    ssize_t i, j;
    long int ret;
    /* arg */
    int fileno, utf8, noreply;
    double timeout;
    /* build */
    int args_offset = 1;
    int fig;
    ssize_t pipeline_len = 1;
    ssize_t request_buf_len = 1024;
    ssize_t request_len = 0;
    STRLEN request_arg_len;
    char * request;
    char * request_arg;
    AV * request_arg_list;
    /* send */
    ssize_t written;
    char * write_buf;
    /* response */
    ssize_t read_max = 16*1024;
    ssize_t read_buf_len;
    ssize_t readed;
    ssize_t parse_offset;
    ssize_t parsed_response;
    long int parse_result;
    char * read_buf;
    struct jet_response_st * response_st;
  PPCODE:
    Newx(request, request_buf_len, char);

    fileno = SvIV(*hv_fetch(self, "fileno", strlen("fileno"), 0));
    utf8 = SvIV(*hv_fetch(self, "utf8", strlen("utf8"), 0));
    timeout = SvNV(*hv_fetch(self, "timeout", strlen("timeout"), 0));;
    noreply = SvIV(*hv_fetch(self, "noreply", strlen("noreply"), 0));

    /* build_message */
    if ( SvOK(ST(args_offset)) && SvROK(ST(args_offset))
        && SvTYPE(SvRV(ST(args_offset))) == SVt_PVAV ) {
      /* build_request([qw/set foo bar/],[qw/set bar baz/]) */
      pipeline_len = items - args_offset;
      for( i=args_offset; i < items; i++ ) {
        request_arg_list = (AV *)SvRV(ST(i));
        fig = (int)log10(av_len(request_arg_list)+1) + 1;
        /* 1(*) + fig + 2(crlf)  */
        renewmem(aTHX_ &request, &request_buf_len, 1 + fig + 2);
        request[request_len++] = '*';
        memcat_i(request, &request_len, av_len(request_arg_list)+1);
        request[request_len++] = 13; // \r
        request[request_len++] = 10; // \n
        for (j=0; j<av_len(request_arg_list)+1; j++) {
          request_arg = svpv2char(aTHX_ *av_fetch(request_arg_list,j,0), &request_arg_len, utf8);
          fig = (int)log10(request_arg_len) + 1;
          /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
          renewmem(aTHX_ &request, &request_buf_len, 1 + fig + 2 + request_arg_len + 2);
          request[request_len++] = '$';
          memcat_i(request, &request_len, request_arg_len);
          request[request_len++] = 13; // \r
          request[request_len++] = 10; // \n
          memcat(request, &request_len, request_arg, request_arg_len);
          request[request_len++] = 13; // \r
          request[request_len++] = 10; // \n
        }
      }
    }
    else {
      /* build_request(qw/set bar baz/) */
      fig = (int)log10(items-args_offset) + 1;
      /* 1(*) + fig + 2(crlf)  */
      renewmem(aTHX_ &request, &request_buf_len, 1 + fig + 2);
      request[request_len++] = '*';
      memcat_i(request, &request_len, items-args_offset);
      request[request_len++] = 13; // \r
      request[request_len++] = 10; // \n
      for (j=args_offset; j<items; j++) {
        request_arg = svpv2char(aTHX_ ST(j), &request_arg_len, utf8);
        fig = (int)log10(request_arg_len) + 1;
        /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
        renewmem(aTHX_ &request, &request_buf_len, 1 + fig + 2 + request_arg_len + 2);
        request[request_len++] = '$';
        memcat_i(request, &request_len, request_arg_len);
        request[request_len++] = 13; // \r
        request[request_len++] = 10; // \n
        memcat(request, &request_len, request_arg, request_arg_len);
        request[request_len++] = 13; // \r
        request[request_len++] = 10; // \n
      }
    }
    // printf("==%s--\n",&request[0]);
    // printf("pipeline_len:%d,%d,%ld\n",args_offset,items,pipeline_len);
    /* send request */
    written = 0;
    write_buf = &request[0];
    while ( request_len > written ) {
      ret = _write_timeout(fileno, timeout, write_buf, request_len - written);
      if ( ret < 0 ) {
        break;
      }
      written += ret;
      write_buf = &request[written];
    }
    /* request done */
    Safefree(request);
    EXTEND(SP, pipeline_len);
    /* request error */
    if (ret <= 0) {
      for (i=0; i<pipeline_len; i++) {
        data_av = newAV(); // (AV*)sv_2mortal((SV*)newAV());
        (void)av_push(data_av, &PL_sv_undef);
        (void)av_push(data_av, newSVpvf("failed to send message: %s", ( errno != 0 ) ? strerror(errno) : "timeout or disconnected"));
        PUSHs( sv_2mortal(newRV_noinc((SV *) data_av)) );
      }
      (void)hv_delete(self, "fileno", strlen("fileno"), 0);
      (void)hv_delete(self, "sock", strlen("socket"), 0);
      goto COMMAND_DONE;
    }
    if ( noreply > 0 ) {
      ret = read(fileno, NULL, read_max);
      for (i=0; i<pipeline_len; i++) {
        data_av = newAV(); // (AV*)sv_2mortal((SV*)newAV());
        (void)av_push(data_av, newSVpvn("0 but true",10));
        PUSHs( sv_2mortal(newRV_noinc((SV *) data_av)) );
      }
      goto COMMAND_DONE;
    }

    /* read response */
    read_buf_len = read_max;
    Newx(read_buf, read_buf_len, char);
    Newx(response_st, sizeof(struct jet_response_st)*pipeline_len, struct jet_response_st);
    parsed_response=0;
    parse_offset=0;
    readed = 0;
    while (1) {
      ret = _read_timeout(fileno, timeout, &read_buf[readed], read_max);
      if ( ret <= 0 ) {
        /* timeout or error */
        for (i=0; i<pipeline_len; i++) {
          data_av = newAV();
          (void)av_push(data_av, &PL_sv_undef);
          (void)av_push(data_av, newSVpvf("failed to read message: %s", ( errno != 0 ) ? strerror(errno) : "timeout or disconnected"));
          response_st[i].data = data_av;
        }
        (void)hv_delete(self, "fileno", strlen("fileno"), 0);
        (void)hv_delete(self, "sock", strlen("socket"), 0);
        goto PARSER_DONE;
      }
      readed += ret;
      while ( readed > parse_offset ) {
        data_av = newAV();
        parse_result = _parse_message(aTHX_ &read_buf[parse_offset], readed - parse_offset, data_av, utf8);
        if ( parse_result == -1 ) {
          /* corruption */
          for (i=0; i<pipeline_len; i++) {
            data_av = newAV();
            (void)av_push(data_av, &PL_sv_undef);
            (void)av_push(data_av, newSVpv("failed to read message: corrupted message found",0));
            response_st[i].data = data_av;
          }
          (void)hv_delete(self, "fileno", strlen("fileno"), 0);
          (void)hv_delete(self, "sock", strlen("socket"), 0);
          goto PARSER_DONE;
        }
        else if ( parse_result == -2 ) {
          break;
        }
        else {
          parse_offset += parse_result;
          response_st[parsed_response++].data = data_av;
        }
      }
      if ( parsed_response >= pipeline_len ) {
        break;
      }
      renewmem(aTHX_ &read_buf, &read_buf_len, readed + read_max);
    }
    PARSER_DONE:
    for (i=0; i<pipeline_len; i++) {
      PUSHs( sv_2mortal(newRV_noinc((SV *)response_st[i].data)) );
    }
    Safefree(read_buf);
    Safefree(response_st);
    COMMAND_DONE:
    XSRETURN(pipeline_len);
