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

static
ssize_t
_build_message(pTHX_ char * dest, ssize_t * dest_size, AV * av_list, const int utf8) {
  STRLEN command_arg_len;
  char *command_arg_src;
  ssize_t dest_len = 0;
  ssize_t i;
  ssize_t j;
  ssize_t fig = 0;
  ssize_t command_len = 1;
  AV * a_list;
  SV *command_arg;
  
  if ( SvOK(*av_fetch(av_list,0,0)) && SvROK(*av_fetch(av_list,0,0))
  && SvTYPE(SvRV(*av_fetch(av_list,0,0))) == SVt_PVAV ) {
    /* build_request([qw/set foo bar/],[qw/set bar baz/]) */
    command_len = av_len(av_list)+1;
    for( j=0; j < av_len(av_list)+1; j++ ) {
      a_list = (AV *)SvRV(*av_fetch(av_list,j,0));
      fig = (int)log10(av_len(a_list)+1) + 1;
      dest[dest_len++] = '*';
      memcat_i(dest, &dest_len, av_len(a_list)+1);
      dest[dest_len++] = 13; // \r
      dest[dest_len++] = 10; // \n
      for (i=0; i<av_len(a_list)+1; i++) {
        command_arg = *av_fetch(a_list,i,0);
        command_arg_src = svpv2char(aTHX_ command_arg, &command_arg_len, utf8);
        fig = (int)log10(command_arg_len) + 1;
        /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
        renewmem(aTHX_ &dest, &*dest_size, 1 + fig + 2 + command_arg_len + 2);
        dest[dest_len++] = '$';
        memcat_i(dest, &dest_len, command_arg_len);
        dest[dest_len++] = 13; // \r
        dest[dest_len++] = 10; // \n
        memcat(dest, &dest_len, command_arg_src, command_arg_len);
        dest[dest_len++] = 13; // \r
        dest[dest_len++] = 10; // \n
      }
    }
  }
  else {
    /* build_request(qw/set bar baz/)
    $msg .= '*'.scalar(@_).$CRLF;
    for my $m (@_) {
      utf8::encode($m) if $self->{utf8};
      $msg .= '$'.length($m).$CRLF.$m.$CRLF;
    }
    */
    fig = (int)log10(av_len(av_list)+1) + 1;
    dest[dest_len++] = '*';
    memcat_i(dest, &dest_len, av_len(av_list)+1);
    dest[dest_len++] = 13; // \r
    dest[dest_len++] = 10; // \n

    for( i=0; i < av_len(av_list)+1; i++ ) {
      command_arg = *av_fetch(av_list,i,0);
      command_arg_src = svpv2char(aTHX_ command_arg, &command_arg_len, utf8);
      fig = (int)log10(command_arg_len) + 1;
      /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
      renewmem(aTHX_ &dest, &*dest_size, 1 + fig + 2 + command_arg_len + 2);
      dest[dest_len++] = '$';
      memcat_i(dest, &dest_len, command_arg_len);
      dest[dest_len++] = 13; // \r
      dest[dest_len++] = 10; // \n
      memcat(dest, &dest_len, command_arg_src, command_arg_len);
      dest[dest_len++] = 13; // \r
      dest[dest_len++] = 10; // \n
    }
  }
  *dest_size = dest_len;
  return command_len;
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
    if ( rv > 0 ) {
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
    if ( rv > 0 ) {
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
build_message(...)
  ALIAS:
    Redis::Jet::build_message = 0
    Redis::Jet::build_message_utf8 = 1
  PREINIT:
    ssize_t i;
    ssize_t message_len = 1024;
    AV *av_list;
    char * message;
  CODE:
    av_list = newAV();
    for (i=0; i < items; i++ ) {
      av_push(av_list, ST(i));
    }
    Newx(message, message_len, char);
    _build_message(aTHX_ message, &message_len, av_list, ix);
    RETVAL = newSVpvn(message, message_len);
    SvPOK_only(RETVAL);
    Safefree(message);
  OUTPUT:
    RETVAL

SV *
send_message(fileno, timeout, ...)
    int fileno
    double timeout
  ALIAS:
    Redis::Jet::send_message = 0
    Redis::Jet::send_message_utf8 = 1
  PREINIT:
    ssize_t i;
    ssize_t message_len = 1024;
    ssize_t written;
    ssize_t write_off;
    ssize_t write_len;
    AV *av_list;
    char * message;
    char * write_buf;
  CODE:
    Newx(message, message_len, char);
    av_list = newAV();
    for (i=2; i < items; i++ ) {
      av_push(av_list, ST(i));
    }
    _build_message(aTHX_ message, &message_len, av_list, ix);
    written = 0;
    write_off = 0;
    write_buf = &message[0];
    while ( (write_len = message_len - write_off) > 0 ) {
      written = _write_timeout(fileno, timeout, write_buf, write_len);
      if ( written < 0 ) {
        break;
      }
      write_off += written;
      write_buf = &message[write_off];
    }

    if (written < 0) {
      ST(0) = sv_newmortal();
      sv_setnv( ST(0), (unsigned long) -1);
    }
    else {
      ST(0) = sv_newmortal();
      sv_setnv( ST(0), (unsigned long) write_off);
    }
    Safefree(message);

ssize_t
phantom_read(fileno)
    int fileno
  PREINIT:
    int buf_size=131072;
  CODE:
    RETVAL = read(fileno, NULL, buf_size);
  OUTPUT:
    RETVAL

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
read_message(fileno, timeout, av_list, required)
    int fileno
    double timeout
    AV * av_list
    ssize_t required
  ALIAS:
    Redis::Jet::read_message = 0
    Redis::Jet::read_message_utf8 = 1
  PREINIT:
    int has_error=0;
    long int read_max=1024*16;
    long int read_buf_len=0;
    long int buf_len;
    long int ret;
    ssize_t parse_result;
    ssize_t parse_offset = 0;
    char *read_buf;
    AV *data_av;
    fd_set rfds;
    struct timeval tv;
  CODE:
    Newx(read_buf, read_max, char);
    buf_len = read_max;
    while (1) {
      ret = _read_timeout(fileno, timeout, &read_buf[read_buf_len], read_max);
      if ( ret < 0 ) {
        /* timeout */
        has_error = -2;
        goto do_result;
      }
      read_buf_len += ret;
      while ( read_buf_len > parse_offset ) {
        data_av = newAV();
        parse_result = _parse_message(aTHX_ &read_buf[parse_offset], read_buf_len - parse_offset, data_av, ix);
        if ( parse_result == -1 ) {
          /* corruption */
          has_error = -1;
          goto do_result;
        }
        else if ( parse_result == -2 ) {
          break;
        }
        else {
          parse_offset += parse_result;
          av_push(av_list, newRV_noinc((SV *) data_av));
        }
      }
      if ( av_len(av_list) + 1 >= required ) {
        break;
      }
      renewmem(aTHX_ &read_buf, &buf_len, read_buf_len + read_max);

    }
    do_result:
    /*
     == -2 timeout or connection error
     == -1 message corruption
    */
    if ( has_error < 0 ) {
      RETVAL = newSViv(has_error);
    }
    else {
      RETVAL = newSViv(read_buf_len);
    }
    Safefree(read_buf);
  OUTPUT:
    RETVAL

void
run_command(self,...)
    HV * self
  PREINIT:
    int fileno;
    int utf8;
    double timeout;
    int noreply;
    ssize_t i;
    ssize_t message_len = 1024;
    ssize_t write_off;
    ssize_t write_len;
    ssize_t buf_len;
    ssize_t command_len;
    AV * req_list;
    AV * res_list;
    AV * data_av;
    char * message;
    char * read_buf;
    char * write_buf;
    long int read_max=1024*16;
    long int read_buf_len=0;
    long int ret;
    ssize_t parse_result;
    ssize_t parse_offset = 0;
  PPCODE:
    Newx(message, message_len, char);
    Newx(read_buf, read_max, char);

    fileno = SvIV(*hv_fetch(self, "fileno", strlen("fileno"), 0));
    utf8 = SvIV(*hv_fetch(self, "utf8", strlen("utf8"), 0));
    timeout = SvNV(*hv_fetch(self, "timeout", strlen("timeout"), 0));;
    noreply = SvIV(*hv_fetch(self, "noreply", strlen("noreply"), 0));

    // printf("fileno:%d utf8:%d timeout:%f noreply:%d\n", fileno, utf8, timeout, noreply);

    res_list = newAV();
    req_list = newAV();

    for (i=1; i < items; i++ ) {
      av_push(req_list, ST(i));
    }
    command_len = _build_message(aTHX_ message, &message_len, req_list, utf8);
    write_off = 0;
    write_buf = &message[0];
    while ( (write_len = message_len - write_off) > 0 ) {
      ret = _write_timeout(fileno, timeout, write_buf, write_len);
      if ( ret < 0 ) {
        break;
      }
      write_off += ret;
      write_buf = &message[write_off];
    }

    if (ret < 0) {
      /* error */
      // av_clear(res_list);
      data_av = newAV();
      (void)av_push(data_av, &PL_sv_undef);
      (void)av_push(data_av, newSVpvf("failed to send message: %s", ( errno != 0 ) ? strerror(errno) : "timeout"));
      for (i=0; i<command_len; i++) {
        (void)av_push(res_list, newRV_noinc((SV *) data_av));
      }
      (void)hv_delete(self, "fileno", strlen("fileno"), 0);
      (void)hv_delete(self, "sock", strlen("socket"), 0);
      goto COMMAND_DONE;
    }
    if ( noreply > 0 ) {
      ret = read(fileno, NULL, read_max);
      // av_clear(res_list);
      data_av = newAV();
      (void)av_push(data_av, newSVpv("0 but true",0));
      for (i=0; i<command_len; i++) {
        (void)av_push(res_list, newRV_noinc((SV *) data_av));
      }
      goto COMMAND_DONE;
    }
    /* read_response */
    buf_len = read_max;
    while (1) {
      ret = _read_timeout(fileno, timeout, &read_buf[read_buf_len], read_max);
      if ( ret < 0 ) {
        /* timeout or error */
        av_clear(res_list);
        data_av = newAV();
        (void)av_push(data_av, &PL_sv_undef);
        (void)av_push(data_av, newSVpvf("failed to read message: %s", ( errno != 0 ) ? strerror(errno) : "timeout"));
        for (i=0; i<command_len; i++) {
          (void)av_push(res_list, newRV_noinc((SV *) data_av));
        }
        (void)hv_delete(self, "fileno", strlen("fileno"), 0);
        (void)hv_delete(self, "sock", strlen("socket"), 0);
        goto COMMAND_DONE;
      }
      read_buf_len += ret;
      while ( read_buf_len > parse_offset ) {
        data_av = newAV();
        parse_result = _parse_message(aTHX_ &read_buf[parse_offset], read_buf_len - parse_offset, data_av, utf8);
        if ( parse_result == -1 ) {
          /* corruption */
          av_clear(res_list);
          data_av = newAV();
          (void)av_push(data_av, &PL_sv_undef);
          (void)av_push(data_av, newSVpv("failed to read message: corrupted message found",0));
          for (i=0; i<command_len; i++) {
            (void)av_push(res_list, newRV_noinc((SV *) data_av));
          }
          (void)hv_delete(self, "fileno", strlen("fileno"), 0);
          (void)hv_delete(self, "sock", strlen("socket"), 0);
          goto COMMAND_DONE;
        }
        else if ( parse_result == -2 ) {
          break;
        }
        else {
          parse_offset += parse_result;
          (void)av_push(res_list, newRV_noinc((SV *) data_av));
        }
      }
      if ( av_len(res_list) + 1 >= command_len ) {
        break;
      }
      renewmem(aTHX_ &read_buf, &buf_len, read_buf_len + read_max);
    }
    
    COMMAND_DONE:
    for (i=0; i<command_len; i++) {
      SV **d = av_fetch(res_list,i,0);
      XPUSHs(*d);
    }
    Safefree(message);
    Safefree(read_buf);
