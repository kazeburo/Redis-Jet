package builder::MyBuilder;
use strict;
use warnings;
use utf8;
use 5.010_001;
use parent qw(Module::Build);
use Devel::CheckCompiler 0.04;

sub new {
    my $self = shift;
    if ( $^O =~ m!(?:MSWin32|cygwin)! ) {
        print "This module does not support Windows.\n";
        exit 0;
    }
    if (check_compile(<<'...', executable => 1) != 1) {
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <poll.h>

int main(void)
{
    struct pollfd wfds[1];
    wfds[0].fd = 1;
    wfds[0].events = POLLOUT;
    return poll(wfds, 1, 5000);
}
...
        print "This platform does not support poll(2).\n";
        exit 0;
    }
    $self->SUPER::new(@_);
}


1;
